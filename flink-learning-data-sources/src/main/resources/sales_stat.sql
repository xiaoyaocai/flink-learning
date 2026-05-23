CREATE DATABASE IF NOT EXISTS trino DEFAULT CHARACTER SET utf8mb4;

USE trino;

CREATE TABLE IF NOT EXISTS sales_stat (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    stat_type       VARCHAR(20)  NOT NULL COMMENT 'PLATFORM-平台订单统计 SKU-SKU销量统计',
    platform        VARCHAR(64)  DEFAULT NULL COMMENT '平台',
    sku             VARCHAR(64)  DEFAULT NULL COMMENT 'SKU',
    order_count     INT          DEFAULT 0 COMMENT '订单数量',
    sales_quantity  INT          DEFAULT 0 COMMENT '销售数量',
    sales_amount    DECIMAL(14, 2) DEFAULT 0.00 COMMENT '销售金额',
    window_start    BIGINT       NOT NULL COMMENT '统计窗口开始时间戳(ms)',
    window_end      BIGINT       NOT NULL COMMENT '统计窗口结束时间戳(ms)',
    update_time     TIMESTAMP    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_sales_stat (stat_type, platform, sku, window_start)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='销量统计表';
