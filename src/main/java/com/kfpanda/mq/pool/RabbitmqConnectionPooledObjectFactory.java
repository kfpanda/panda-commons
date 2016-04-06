package com.kfpanda.mq.pool;

import com.kfpanda.mq.pool1.PoolConfig;
import com.kfpanda.util.PropertiesUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by kfpanda on 16-4-5.
 */
public class RabbitmqConnectionPooledObjectFactory extends BasePooledObjectFactory<Connection> {
    private static Logger logger = LoggerFactory.getLogger(RabbitmqConnectionPooledObjectFactory.class);

    protected static String host;
    protected static int port;
    protected static int DEFAULT_PORT = 5672;
    protected static String virtualHost;
    protected static int timeout;
    protected static String userName;
    protected static String password;

    private static final ConnectionFactory factory = new ConnectionFactory();

    @Override
    public Connection create() throws Exception {
        Properties prop = PropertiesUtil.getConfig();

        String tmout = prop.getProperty("rabbitmq.timeout");
        host = prop.getProperty("rabbitmq.host");
        port = prop.getProperty("rabbitmq.port") == null
                ? DEFAULT_PORT : Integer.valueOf(prop.getProperty("rabbitmq.port"));
        virtualHost = prop.getProperty("rabbitmq.virtualHost");
        timeout = tmout == null ? 0 : Integer.valueOf(tmout);
        userName = prop.getProperty("rabbitmq.userName");
        password = prop.getProperty("rabbitmq.password");

        factory.setHost(host);
        factory.setPort(port);
        if(userName != null && "".equals(userName)){
            factory.setUsername(userName);
        }
        if(password != null && "".equals(password)){
            factory.setPassword(password);
        }
        factory.setAutomaticRecoveryEnabled(true);
//        factory.setRequestedChannelMax(50);
        if(timeout > 0){
            factory.setConnectionTimeout(timeout);
        }
        if(virtualHost != null && "".equals(virtualHost)){
            factory.setVirtualHost(virtualHost);
        }

        Connection conn = null;
        try {
            conn = factory.newConnection();
        } catch (IOException e) {
            logger.error("rabbitmq factory new connection error.", e);
        }
        return conn;
    }

    @Override
    public PooledObject<Connection> wrap(Connection connection) {
        return new DefaultPooledObject<Connection>(connection);
    }
}
