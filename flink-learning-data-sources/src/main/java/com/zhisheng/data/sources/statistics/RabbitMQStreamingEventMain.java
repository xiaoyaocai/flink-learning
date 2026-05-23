package com.zhisheng.data.sources.statistics;

import com.google.common.collect.Lists;
import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.data.sources.statistics.config.DemoConfig;
import com.zhisheng.data.sources.statistics.model.FbmOrderEvent;
import com.zhisheng.data.sources.statistics.model.OrderLine;
import com.zhisheng.data.sources.statistics.model.SalesStat;
import com.zhisheng.data.sources.statistics.sink.SalesStatMySQLSink;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.Collector;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 从 RabbitMQ 持续消费 FBM 订单已发货事件，按分钟统计平台订单数、SKU 销量与销售额，并写入 MySQL。
 * <p>
 * 部署环境：Flink 1.14.6；连接信息见 resources/docker-compose.yml；运行参数见 resources/demo.json。
 */
@Slf4j
public class RabbitMQStreamingEventMain {

    public static void main(String[] args) throws Exception {
        DemoConfig config = loadConfig();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(60_000L);

        RMQConnectionConfig connectionConfig = buildRabbitMQConfig(config);

        // 第三个参数为 usesCorrelationId：发送端未设置 correlationId 时必须为 false
        boolean usesCorrelationId = config.getRabbitmq().isUsesCorrelationId();
        DataStream<String> orderStream = env
                .addSource(new RMQSource<>(
                        connectionConfig,
                        config.getRabbitmq().getQueue(),
                        usesCorrelationId,
                        new SimpleStringSchema()))
                .setParallelism(1)
                .name("rabbitmq-fbm-order-shipped");

        DataStream<OrderLine> orderLineStream = orderStream
                .map((MapFunction<String, FbmOrderEvent>) json -> GsonUtil.fromJson(json, FbmOrderEvent.class))
                .filter(event -> event != null && event.getOrderId() != null)
                .flatMap(new OrderLineFlatMap())
                .name("parse-order-lines");

        orderLineStream
                .timeWindowAll(Time.minutes(config.getWindowMinutes()))
                .apply(new SalesStatAggregator())
                .addSink(new SalesStatMySQLSink(config.getMysql()))
                .setParallelism(1)
                .name("mysql-sales-stat-sink");

        env.execute("rabbitmq-fbm-order-shipped-statistics");
    }

    private static RMQConnectionConfig buildRabbitMQConfig(DemoConfig config) {
        DemoConfig.RabbitMQConfig rabbitmq = config.getRabbitmq();
        return new RMQConnectionConfig.Builder()
                .setHost(rabbitmq.getHost())
                .setPort(rabbitmq.getPort())
                .setVirtualHost(rabbitmq.getVirtualHost())
                .setUserName(rabbitmq.getUsername())
                .setPassword(rabbitmq.getPassword())
                .build();
    }

    private static DemoConfig loadConfig() {
        try (InputStream inputStream = RabbitMQStreamingEventMain.class.getResourceAsStream("/demo.json")) {
            if (inputStream == null) {
                throw new IllegalStateException("demo.json not found in classpath");
            }
            StringBuilder json = new StringBuilder();
            try (InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
                char[] buffer = new char[4096];
                int length;
                while ((length = reader.read(buffer)) != -1) {
                    json.append(buffer, 0, length);
                }
            }
            return GsonUtil.fromJson(json.toString(), DemoConfig.class);
        } catch (Exception e) {
            throw new IllegalStateException("failed to load demo.json", e);
        }
    }

    /**
     * 将订单按 items 展开为 SKU 行
     */
    private static class OrderLineFlatMap implements FlatMapFunction<FbmOrderEvent, OrderLine> {

        @Override
        public void flatMap(FbmOrderEvent event, Collector<OrderLine> out) {
            if (event.getItems() == null || event.getItems().isEmpty()) {
                return;
            }
            String platform = event.getPlatform() == null ? "UNKNOWN" : event.getPlatform();
            for (FbmOrderEvent.OrderItem item : event.getItems()) {
                if (item.getSku() == null) {
                    continue;
                }
                int quantity = Math.max(item.getQuantity(), 0);
                double lineAmount = quantity * item.getUnitPrice();
                out.collect(new OrderLine(event.getOrderId(), platform, item.getSku(), quantity, lineAmount));
            }
        }
    }

    /**
     * 1 分钟窗口内聚合：平台订单数、SKU 销量、SKU 销售额
     */
    private static class SalesStatAggregator implements AllWindowFunction<OrderLine, List<SalesStat>, TimeWindow> {

        @Override
        public void apply(TimeWindow window, Iterable<OrderLine> values, Collector<List<SalesStat>> out) {
            List<OrderLine> lines = Lists.newArrayList(values);
            if (lines.isEmpty()) {
                return;
            }

            Map<String, Set<String>> platformOrders = new HashMap<>();
            Map<String, Integer> skuQuantity = new HashMap<>();
            Map<String, Double> skuAmount = new HashMap<>();

            for (OrderLine line : lines) {
                platformOrders
                        .computeIfAbsent(line.getPlatform(), key -> new HashSet<>())
                        .add(line.getOrderId());
                skuQuantity.merge(line.getSku(), line.getQuantity(), Integer::sum);
                skuAmount.merge(line.getSku(), line.getLineAmount(), Double::sum);
            }

            long windowStart = window.getStart();
            long windowEnd = window.getEnd();
            List<SalesStat> stats = new ArrayList<>();

            platformOrders.forEach((platform, orderIds) -> stats.add(SalesStat.builder()
                    .statType(SalesStat.TYPE_PLATFORM)
                    .platform(platform)
                    .orderCount(orderIds.size())
                    .windowStart(windowStart)
                    .windowEnd(windowEnd)
                    .build()));

            skuQuantity.forEach((sku, quantity) -> stats.add(SalesStat.builder()
                    .statType(SalesStat.TYPE_SKU)
                    .sku(sku)
                    .salesQuantity(quantity)
                    .salesAmount(skuAmount.getOrDefault(sku, 0D))
                    .windowStart(windowStart)
                    .windowEnd(windowEnd)
                    .build()));

            log.info("窗口 [{} - {}] 统计 {} 条指标", windowStart, windowEnd, stats.size());
            out.collect(stats);
        }
    }
}
