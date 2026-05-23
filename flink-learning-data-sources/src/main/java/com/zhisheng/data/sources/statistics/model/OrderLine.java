package com.zhisheng.data.sources.statistics.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 按 SKU 展开后的订单行，用于统计
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderLine implements Serializable {

    private static final long serialVersionUID = 1L;

    private String orderId;
    private String platform;
    private String sku;
    private int quantity;
    private double lineAmount;
}
