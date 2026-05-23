package com.zhisheng.data.sources.statistics;

public class RabbitMQStreamingEventMain {
    public static void main(String[] args) {
        //开发部署环境基于 flink:1.14.6-scala_2.12-java8，订单事件数据格式如下：resources/order.json
        //todo Step1:Flink连接RabbitMQ  192.168.184.130 请读取 resources/docker-compose.yml 文件，里面有 RabbitMQ,MySQL,Mongodb 的账号密码等连接信息
        //todo Step2:连接RabbitMQ topic : fbm_order_shipped
        //todo Step3:接收参数：demo.json
        //todo Step4:统计每个平台的订单数量，每个SKU的销售数量，销售金额
        //todo Step5:结果保存到MySQL trino库，销量表中
        //todo Step6:Flink持续统计RabbitMQ的订单已发货事件数据，每一分钟更新统计到MySQL中
    }
}
