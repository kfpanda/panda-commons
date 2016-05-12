
/**  
 * rabbitmq 消费者接口。
 * @author kfpanda
*/  
package com.kfpanda.mq.svr;

import com.rabbitmq.client.Channel;

/**
 * 消费者接口
 * @author kfpanda
 * @date 2016年3月28日 下午4:54:54
 */
public interface Consumer {
    
    /**
     * 消息消费方法
     * @param message 消息内容
     */
    Boolean consumeMsg(byte[] message, Channel channel);
    
}