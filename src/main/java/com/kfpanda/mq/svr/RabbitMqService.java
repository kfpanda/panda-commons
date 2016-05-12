
/**  
 * Project Name:atm-message  
 * File Name:RabbitMqService.java  
 * Package Name:com.awifi.core.basic.service.mq  
 * Date:2016年3月28日下午12:41:02  
 * Copyright (c) 2016, 中国电信爱WiFi运营中心
 *  
 */  
package com.kfpanda.mq.svr;

import com.kfpanda.mq.RabbitmqUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;

/**
 * RabbitMq操作类
 * @author kfpanda
 * @date 2016年3月28日 下午12:41:08
 */
public class RabbitMqService {
    private static Logger logger = LogManager.getLogger(RabbitMqService.class);

    /**
     * 将消息插入消息队列(routing)，抛出异常
     * @param exchange exchange名称，为空则使用默认exchange
     * @param routingKey 路由key，不能为空
     * @param message 消息主体，不为空
     * @param durable 是否持久化
     */
    public void directPublish(final String exchange, final String routingKey, final String type, final String message,
                              final boolean durable) throws Exception {
        Channel channel = RabbitmqUtil.borrowObject();
//        channel.exchangeDeclare(exchange, type, durable);
        channel.basicPublish(exchange, routingKey, null, message.getBytes());
        logger.debug("publish success, msg = ({}), type = ({}), routingKey = ({}), exchange = ({})", message, type, routingKey, exchange);
        RabbitmqUtil.returnObject(channel);
    }

    /**
     * 将批量消息插入消息队列(routing),不抛出异常，返回插入失败数据
     * @param exchange exchange名称，为空则使用默认exchange
     * @param routingKey routing key
     * @param message 消息主体，不为空
     * @param durable 是否持久化
     * @return 插入失败数据
     */
    public String[] directPublish(String exchange, String routingKey, String[] message, boolean durable) {
        return null;
    }

    /**
     * 将消息插入消息队列(pub/sub)，抛出异常
     * @param exchange exchange名称，为空则使用默认exchange
     * @param message 消息
     * @param durable 是否持久化
     * @throws Exception
     */
    public void pubPublish(String exchange, String message, boolean durable) throws Exception {
        directPublish(exchange, "", "fanout", message, durable);
    }

    /**
     * 将批量消息插入消息队列(pub/sub),不抛出异常，返回插入失败数据
     * @param exchange exchange名称，为空则使用默认exchange
     * @param message 消息
     * @param durable 是否持久化
     * @return 插入失败数据
     */
    public String[] pubPublish(String exchange, String[] message, boolean durable) {
        return null;
    }

    /**
     * 消费消息(路由模式)，需实现Consumer接口
     * @param queueName 队列名 不能为空
     * @param exchange exchange名称，为空则使用默认exchange
     * @param routingKey routing key
     * @param ack 消息是否需要手动确认，true则为手动确认（当consumeMsg消息返回true时，则发送一个ack消息给MQ，否则MQ将消息作为新消息重新放入队列）。
     * false 则为自动确认。
     * @param consumer 根据consumer.consumeMsg() 方法处理消息
     * @throws Exception
     */
    public void directConsume(final String queueName, final String exchange, final String routingKey,
            final boolean ack, final Consumer consumer) throws Exception {
        Channel channel = RabbitmqUtil.borrowObject();
//        channel.exchangeDeclare(exchange, "direct");
//        channel.queueBind(queueName, exchange, routingKey);
        channel.basicQos(1); // 公平分发
        QueueingConsumer queueingConsumer = new QueueingConsumer(channel);
        channel.basicConsume(queueName, !ack, queueingConsumer);
        while (true) {
            consumerMsg(queueingConsumer, channel, consumer, ack);
        }
    }

    /**
     * 消费消息(pub/sub模式)，需实现Consumer接口
     * @param queueName 队列名,不能为空
     * @param exchange exchange名称，为空则使用默认exchange
     * @param consumer Consumer接口实现类，根据consumeMsg()
     * @param ack 消息是否需要手动确认，true则为手动确认（当consumeMsg消息返回true时，则发送一个ack消息给MQ，否则MQ将消息作为新消息重新放入队列）。
     * false 则为自动确认。
     * @throws Exception
     */
    public void subConsume(String queueName, String exchange, boolean ack, Consumer consumer)
            throws Exception {
        Channel channel = RabbitmqUtil.borrowObject();
//        channel.exchangeDeclare(exchange, "fanout");
//        channel.queueBind(queueName, exchange, "");
        channel.basicQos(1); // 公平分发
        QueueingConsumer queueingConsumer = new QueueingConsumer(channel);
        channel.basicConsume(queueName, !ack, queueingConsumer);
        while (true) {
            consumerMsg(queueingConsumer, channel, consumer, ack);
        }
    }

    /**
     * consumerMsg:(这里用一句话描述这个方法的作用). <br/>
     * @param queueingConsumer
     * @param channel
     * @param consumer
     * @param ack
     * @throws Exception
     * @author kfpanda
     */
    private void consumerMsg(QueueingConsumer queueingConsumer, Channel channel, Consumer consumer, boolean ack)
            throws Exception {
        QueueingConsumer.Delivery delivery = queueingConsumer.nextDelivery();
        Boolean rsl = consumer.consumeMsg(delivery.getBody(), channel);
        if(ack){
            if(null == rsl || rsl){
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }else{
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
            }
        }
    }


}