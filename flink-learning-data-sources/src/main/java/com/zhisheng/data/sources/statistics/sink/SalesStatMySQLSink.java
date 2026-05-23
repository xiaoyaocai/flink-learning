package com.zhisheng.data.sources.statistics.sink;

import com.zhisheng.data.sources.statistics.config.DemoConfig;
import com.zhisheng.data.sources.statistics.model.SalesStat;
import com.zhisheng.data.sources.utils.MySQLUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.io.Serializable;
import java.util.List;

/**
 * 将分钟窗口统计结果写入 MySQL trino.sales_stat
 */
@Slf4j
public class SalesStatMySQLSink extends RichSinkFunction<List<SalesStat>> implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String UPSERT_SQL =
            "INSERT INTO sales_stat (stat_type, platform, sku, order_count, sales_quantity, sales_amount, window_start, window_end) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                    + "ON DUPLICATE KEY UPDATE "
                    + "order_count = VALUES(order_count), "
                    + "sales_quantity = VALUES(sales_quantity), "
                    + "sales_amount = VALUES(sales_amount), "
                    + "update_time = CURRENT_TIMESTAMP";

    private final DemoConfig.MysqlConfig mysqlConfig;
    private transient Connection connection;
    private transient PreparedStatement preparedStatement;

    public SalesStatMySQLSink(DemoConfig.MysqlConfig mysqlConfig) {
        this.mysqlConfig = mysqlConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = MySQLUtil.getConnection(
                mysqlConfig.getDriver(),
                mysqlConfig.getUrl(),
                mysqlConfig.getUsername(),
                mysqlConfig.getPassword());
        if (connection == null) {
            throw new IllegalStateException(
                    "无法连接 MySQL，请检查 demo.json 中 mysql 配置：服务是否启动、地址/端口/库名/账号密码是否正确，"
                            + "并确认已执行 sales_stat.sql 创建 trino 库与 sales_stat 表。url="
                            + mysqlConfig.getUrl());
        }
        preparedStatement = connection.prepareStatement(UPSERT_SQL);
    }

    @Override
    public void invoke(List<SalesStat> stats, Context context) throws Exception {
        if (preparedStatement == null || stats == null || stats.isEmpty()) {
            return;
        }
        for (SalesStat stat : stats) {
            preparedStatement.setString(1, stat.getStatType());
            preparedStatement.setString(2, emptyToNull(stat.getPlatform()));
            preparedStatement.setString(3, emptyToNull(stat.getSku()));
            preparedStatement.setInt(4, stat.getOrderCount());
            preparedStatement.setInt(5, stat.getSalesQuantity());
            preparedStatement.setBigDecimal(6, BigDecimal.valueOf(stat.getSalesAmount())
                    .setScale(2, RoundingMode.HALF_UP));
            preparedStatement.setLong(7, stat.getWindowStart());
            preparedStatement.setLong(8, stat.getWindowEnd());
            preparedStatement.addBatch();
        }
        int[] counts = preparedStatement.executeBatch();
        log.info("写入销量统计 {} 条", counts.length);
    }

    @Override
    public void close() throws Exception {
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.close();
    }

    private static String emptyToNull(String value) {
        return value == null || value.isEmpty() ? null : value;
    }
}
