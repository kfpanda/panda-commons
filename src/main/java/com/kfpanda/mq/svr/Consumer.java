
/**  
 * Project Name:atm-message  
 * File Name:Consume.java  
 * Package Name:com.awifi.core.basic.service.mq  
 * Date:2016年3月28日下午4:54:48  
 * Copyright (c) 2016, 中国电信爱WiFi运营中心
 *  
*/  
package com.kfpanda.mq.svr;

import com.rabbitmq.client.Channel;

/**
 * 
 * @Title: Consume.java
 * @Description: 接入系统平台类文件
 * @Copyright: 中国电信爱WiFi运营中心
 * @Company: 中国电信爱WiFi运营中心
 * @author wangx
 * @date 2016年3月28日 下午4:54:54
 */
public interface Consumer {
    
    /**
     * 消息消费方法
     * @param message 消息内容
     * @author wangx
     * @date 2016年3月28日 下午4:56:19
     */
    Boolean consumeMsg(byte[] message, Channel channel);
    
}