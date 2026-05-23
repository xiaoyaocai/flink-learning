package com.zhisheng.data.sources.statistics.model;

import lombok.Data;

import java.util.List;

/**
 * FBM 订单已发货事件，数据格式见 resources/order.json
 */
@Data
public class FbmOrderEvent {

    private String orderId;
    private String platform;
    private String orderType;
    private String orderStatus;
    private String createTime;
    private Buyer buyer;
    private ShippingAddress shippingAddress;
    private List<OrderItem> items;
    private Payment payment;
    private Shipping shipping;
    private Seller seller;
    private Metadata metadata;

    @Data
    public static class Buyer {
        private String buyerId;
        private String name;
        private String email;
        private String phone;
    }

    @Data
    public static class ShippingAddress {
        private String country;
        private String state;
        private String city;
        private String addressLine1;
        private String addressLine2;
        private String zipCode;
    }

    @Data
    public static class OrderItem {
        private String sku;
        private String productName;
        private int quantity;
        private double unitPrice;
        private String currency;
    }

    @Data
    public static class Payment {
        private String paymentMethod;
        private boolean paid;
        private double amount;
        private String currency;
    }

    @Data
    public static class Shipping {
        private String shippingMethod;
        private String carrier;
        private String trackingNumber;
        private double shippingCost;
        private String status;
    }

    @Data
    public static class Seller {
        private String sellerId;
        private String warehouse;
    }

    @Data
    public static class Metadata {
        private String source;
        private String priority;
        private List<String> tags;
    }
}
