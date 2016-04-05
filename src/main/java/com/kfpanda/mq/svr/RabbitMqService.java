
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;

/**
 * RabbitMq操作类
 * @Title: RabbitMqService.java
 * @Description: 接入系统平台类文件
 * @Copyright: 中国电信爱WiFi运营中心
 * @Company: 中国电信爱WiFi运营中心
 * @author wangx
 * @date 2016年3月28日 下午12:41:08
 */
public class RabbitMqService {

    /**
     * log4j
     */
    private Logger logger = LoggerFactory.getLogger(RabbitMqService.class);

    /**
     * 将消息插入消息队列(routing)，抛出异常
     * @param exchange exchange名称，为空则使用默认exchange
     * @param routingKey 路由key，不能为空
     * @param message 消息主体，不为空
     * @param durable 是否持久化
     * @author wangx
     * @date 2016年3月28日 下午12:42:56
     */
    public void directPublish(final String exchange, final String routingKey, final String message,
            final boolean durable) throws Exception {
        Channel channel = RabbitmqUtil.getResource();
        channel.exchangeDeclare(exchange, "direct", durable);
        channel.basicPublish(exchange, routingKey, null, message.getBytes());
        logger.debug("publish success, msg = ({}), routingKey = ({}), exchange = ({})", message, routingKey, exchange);
        RabbitmqUtil.returnResource(channel);
    }

    /**
     * 将批量消息插入消息队列(routing),不抛出异常，返回插入失败数据
     * @param exchange exchange名称，为空则使用默认exchange
     * @param routingKey routing key
     * @param message 消息主体，不为空
     * @param durable 是否持久化
     * @return 插入失败数据
     * @author wangx
     * @date 2016年3月28日 下午12:42:56
     */
    public String[] directPublish(String exchange, String routingKey, String[] message, boolean durable) {
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
     * @author wangx
     * @throws Exception 
     * @date 2016年3月28日 下午5:01:07
     */
    public void directConsume(final String queueName, final String exchange, final String routingKey,
            final boolean ack, final Consumer consumer) throws Exception {
        Channel channel = RabbitmqUtil.getResource();
        channel.exchangeDeclare(exchange, "direct");
        channel.queueBind(queueName, exchange, routingKey);
        channel.basicQos(1); // 公平分发
        QueueingConsumer queueingConsumer = new QueueingConsumer(channel);
        channel.basicConsume(queueName, !ack, queueingConsumer);
        while (true) {
            QueueingConsumer.Delivery delivery = queueingConsumer.nextDelivery();
            Boolean rsl = consumer.consumeMsg(delivery.getBody(), channel);
            if (ack && (null == rsl || rsl)) { // 如果非自动确认，手动确认
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } else if (ack && !rsl) {
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
            }
        }
    }

    /**
     * 将消息插入消息队列(pub/sub)，抛出异常
     * @param exchange exchange名称，为空则使用默认exchange
     * @param message 消息
     * @param durable 是否持久化
     * @author wangx
     * @throws Exception 
     * @date 2016年3月28日 下午12:42:56
     */
    public void pubPublish(String exchange, String message, boolean durable) throws Exception {
        Channel channel = RabbitmqUtil.getResource();
        channel.exchangeDeclare(exchange, "fanout", durable);
        channel.basicPublish(exchange, "", null, message.getBytes());
        logger.debug("publish success, msg = ({}), exchange = ({})", message, exchange);
        RabbitmqUtil.returnResource(channel);
    }

    /**
     * 将批量消息插入消息队列(pub/sub),不抛出异常，返回插入失败数据
     * @param exchange exchange名称，为空则使用默认exchange
     * @param message 消息
     * @param durable 是否持久化
     * @return 插入失败数据
     * @author wangx
     * @date 2016年3月28日 下午12:42:56
     */
    public String[] pubPublish(String exchange, String[] message, boolean durable) {
        return null;
    }

    /**
     * 消费消息(pub/sub模式)，需实现Consumer接口
     * @param queueName 队列名,不能为空
     * @param exchange exchange名称，为空则使用默认exchange
     * @param consumer Consumer接口实现类，根据consumeMsg()
     * @param ack 消息是否需要手动确认，true则为手动确认（当consumeMsg消息返回true时，则发送一个ack消息给MQ，否则MQ将消息作为新消息重新放入队列）。
     * false 则为自动确认。
     * @param consumer 根据consumer.consumeMsg() 方法处理消息
     * @author wangx
     * @throws Exception 
     * @date 2016年3月28日 下午3:30:02
     */
    public void subConsume(String queueName, String exchange, boolean ack, Consumer consumer)
            throws Exception {
        Channel channel = RabbitmqUtil.getResource();
        channel.exchangeDeclare(exchange, "fanout");
        channel.queueBind(queueName, exchange, "");
        channel.basicQos(1); // 公平分发
        QueueingConsumer queueingConsumer = new QueueingConsumer(channel);
        channel.basicConsume(queueName, !ack, queueingConsumer);
        while (true) {
            QueueingConsumer.Delivery delivery = queueingConsumer.nextDelivery();
            Boolean rsl = consumer.consumeMsg(delivery.getBody(), channel);
            if (ack && (null == rsl || rsl)) { // 如果非自动确认，手动确认
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } else if (ack && !rsl) {
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
            }
        }
    }
}