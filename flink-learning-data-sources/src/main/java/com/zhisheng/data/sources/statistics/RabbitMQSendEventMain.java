package com.zhisheng.data.sources.statistics;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.data.sources.statistics.config.DemoConfig;
import com.zhisheng.data.sources.statistics.model.FbmOrderEvent;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 读取 order.json 作为订单模板，循环发送 FBM 已发货事件到 RabbitMQ，供 {@link RabbitMQStreamingEventMain} 消费统计。
 */
@Slf4j
public class RabbitMQSendEventMain {

    private static final int SEND_COUNT = 100000;
    private static final double NEW_SKU_PROBABILITY = 0.395;

    public static void main(String[] args) throws Exception {
        DemoConfig config = loadConfig();
        FbmOrderEvent template = loadOrderTemplate();

        DemoConfig.RabbitMQConfig rabbitmq = config.getRabbitmq();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitmq.getHost());
        factory.setPort(rabbitmq.getPort());
        factory.setVirtualHost(rabbitmq.getVirtualHost());
        factory.setUsername(rabbitmq.getUsername());
        factory.setPassword(rabbitmq.getPassword());

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        try {
            for (int i = 1; i <= SEND_COUNT; i++) {
                FbmOrderEvent event = buildOrderEvent(template, i);
                String json = GsonUtil.toJson(event);
                channel.basicPublish("", rabbitmq.getQueue(), null, json.getBytes(StandardCharsets.UTF_8));
                if (i % 100 == 0 || i == SEND_COUNT) {
                    log.info("已发送 {}/{} 条订单事件到队列 {}", i, SEND_COUNT, rabbitmq.getQueue());
                }
            }
        } finally {
            channel.close();
            connection.close();
        }
    }

    private static FbmOrderEvent buildOrderEvent(FbmOrderEvent template, int sequence) {
        FbmOrderEvent event = GsonUtil.fromJson(GsonUtil.toJson(template), FbmOrderEvent.class);
        event.setOrderId(String.format("20260523%04d", sequence));
        event.setOrderStatus("SHIPPED");
        event.setCreateTime(String.format("2026-05-23T10:%02d:%02dZ", sequence / 60, sequence % 60));

        if (event.getShipping() != null) {
            event.getShipping().setStatus("SHIPPED");
            event.getShipping().setTrackingNumber("1Z999AA1012345" + String.format("%04d", sequence));
        }

        double itemsTotal = 0;
        if (event.getItems() != null) {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            for (FbmOrderEvent.OrderItem item : event.getItems()) {
                if (random.nextDouble() < NEW_SKU_PROBABILITY) {
                    item.setSku("SKU-" + String.format("%05d", random.nextInt(10000, 100000)));
                }
                item.setUnitPrice(roundPrice(random.nextDouble(9.99, 199.99)));
                itemsTotal += item.getQuantity() * item.getUnitPrice();
            }
        }

        if (event.getPayment() != null) {
            double shippingCost = event.getShipping() != null ? event.getShipping().getShippingCost() : 0;
            event.getPayment().setAmount(roundPrice(itemsTotal + shippingCost));
        }
        return event;
    }

    private static double roundPrice(double price) {
        return Math.round(price * 100.0) / 100.0;
    }

    private static DemoConfig loadConfig() {
        try (InputStream inputStream = RabbitMQSendEventMain.class.getResourceAsStream("/demo.json")) {
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

    private static FbmOrderEvent loadOrderTemplate() {
        try (InputStream inputStream = RabbitMQSendEventMain.class.getResourceAsStream("/order.json")) {
            if (inputStream == null) {
                throw new IllegalStateException("order.json not found in classpath");
            }
            StringBuilder json = new StringBuilder();
            try (InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
                char[] buffer = new char[4096];
                int length;
                while ((length = reader.read(buffer)) != -1) {
                    json.append(buffer, 0, length);
                }
            }
            FbmOrderEvent event = GsonUtil.fromJson(json.toString(), FbmOrderEvent.class);
            if (event == null || event.getOrderId() == null) {
                throw new IllegalStateException("invalid order.json");
            }
            return event;
        } catch (Exception e) {
            throw new IllegalStateException("failed to load order.json", e);
        }
    }
}
