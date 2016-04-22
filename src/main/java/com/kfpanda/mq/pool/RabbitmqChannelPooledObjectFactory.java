package com.kfpanda.mq.pool;

import com.kfpanda.util.PropertiesUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

/**
 * Created by kfpanda on 16-4-5.
 */
public class RabbitmqChannelPooledObjectFactory extends BasePooledObjectFactory<Channel> {
    private static Logger logger = LogManager.getLogger(RabbitmqConnectionPooledObjectFactory.class);

    private ObjectPool<Connection> connectionPool;
    private static final ConnectionFactory factory = new ConnectionFactory();

    public RabbitmqChannelPooledObjectFactory(RabbitmqConnectionPooledObjectFactory connectionFactory){
        Properties prop = PropertiesUtil.getConfig();
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxIdle(Integer.parseInt(prop.getProperty("rabbitmq.conn.pool.max.idle")));
        poolConfig.setMinIdle(Integer.parseInt(prop.getProperty("rabbitmq.conn.pool.min.idle")));
        poolConfig.setMaxTotal(Integer.parseInt(prop.getProperty("rabbitmq.conn.pool.max.total")));
        poolConfig.setMaxWaitMillis(Integer.parseInt(prop.getProperty("rabbitmq.conn.pool.max.wait")));
        // 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
        poolConfig.setTestOnBorrow(true);

        connectionPool = new GenericObjectPool(new RabbitmqConnectionPooledObjectFactory(), poolConfig);
    }

    @Override
    public Channel create() throws Exception {
        Connection conn = null;
        Channel channel = null;
        try {
            conn = connectionPool.borrowObject();
            channel = conn.createChannel();
        } catch (Exception e) {
            logger.error("rabbitmq borrow connection error.", e);
        }
        return channel;
    }

    @Override
    public PooledObject<Channel> wrap(Channel channel) {
        return new DefaultPooledObject(channel);
    }
}
