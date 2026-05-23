package com.zhisheng.data.sources.statistics;

public class RabbitMQSendEventMain {
    public static void main(String [] args){
        //读取当前 resources/order.json，docker-compose.yml，
        //将order.json 转换成FbmOrderEvent
        //循环发送RabbitMQ 的rabbitmq-fbm-order-shipped队列   每次发送 39.5%随机生成新的SKU，每次生成不同的销售价格
    }
}
