package com.zhisheng.data.sources.statistics.config;

import lombok.Data;

import java.io.Serializable;

/**
 * 运行参数，对应 resources/demo.json
 */
@Data
public class DemoConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private RabbitMQConfig rabbitmq;
    private MysqlConfig mysql;
    private int windowMinutes = 1;

    @Data
    public static class RabbitMQConfig implements Serializable {

        private static final long serialVersionUID = 1L;
        private String host;
        private int port;
        private String virtualHost;
        private String username;
        private String password;
        private String queue;
    }

    @Data
    public static class MysqlConfig implements Serializable {

        private static final long serialVersionUID = 1L;

        private String driver;
        private String url;
        private String username;
        private String password;
    }
}
