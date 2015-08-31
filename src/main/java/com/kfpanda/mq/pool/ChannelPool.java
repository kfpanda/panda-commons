package com.kfpanda.mq.pool;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool.Config;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

public class ChannelPool extends Pool<Channel>{
	
    public ChannelPool(final Connection conn, final String queue) {
        super(new Config(), new ChannelFactory(conn, queue, null, null, null));
    }
    
    public ChannelPool(final Config poolConfig, final Connection conn, final String queue, 
    		final String amqDirect, final String exchangeType, final String routeKey) {
        super(poolConfig, new ChannelFactory(conn, queue, amqDirect, exchangeType, routeKey));
    }

    public ChannelPool(final GenericObjectPool.Config poolConfig,
    		final Connection conn, final String queue) {
    	super(poolConfig, new ChannelFactory(conn, queue, null, null, null));
    }
    
    /**
     * PoolableObjectFactory custom impl.
     */
    private static class ChannelFactory extends BasePoolableObjectFactory {
//    	private static ConnectionFactory factory = new ConnectionFactory();
    	private Connection conn;
//        private String host;
//        private int port;
//        private String virtualHost;
//        private int timeout;
//        private String userName;
//        private String password;
        
        private static final String AMQ_DIRECT = "amq.direct";
    	private static final String EXCHANGE_TYPE = "direct";
    	private static final String ROUTE_KEY = "route.key";
    	private String queue;
        private String amqDirect;
    	private String exchangeType;
    	private String routeKey;
        
        public ChannelFactory(Connection conn, final String queue, final String amqDirect, final String exchangeType,
                final String routeKey) {
            super();
            this.conn = conn;
            this.queue = queue;
            this.amqDirect = amqDirect == null ? AMQ_DIRECT : amqDirect;
    		this.exchangeType = exchangeType == null ? EXCHANGE_TYPE : exchangeType;
    		this.routeKey = routeKey == null ? ROUTE_KEY : routeKey;
            /*this.host = host;
            this.port = port;
            this.virtualHost = virtualHost;
            this.timeout = (timeout > 0) ? timeout : -1;
            this.userName = userName;
            this.password = password;
//          //最大channel的创建数为30个
//            factory.setRequestedChannelMax(30);
			factory.setHost(this.host);
			factory.setPort(this.port);
			if (this.userName != null && this.userName != "") {
				factory.setUsername(this.userName);
			}
			if (this.password != null && this.password != "") {
				factory.setPassword(this.password);
			}
			factory.setAutomaticRecoveryEnabled(true);
			if (this.timeout > 0) {
				factory.setConnectionTimeout(this.timeout);
			}
			if (this.virtualHost != null && this.virtualHost != "") {
				factory.setVirtualHost(this.virtualHost);
			}
			try {
				conn = factory.newConnection();
			} catch (IOException e) {
				e.printStackTrace();
			}*/
        }

        public Object makeObject() throws Exception {
            Channel channel = conn.createChannel();
            channel.exchangeDeclare(amqDirect, exchangeType, true);
	        channel.queueBind(queue, amqDirect, routeKey);
	        return channel;
        }

        public void destroyObject(final Object obj) throws Exception {
            if (obj instanceof Channel) {
                final Channel channel = (Channel) obj;
                if(channel != null && channel.isOpen()){
                	channel.close();
                }
            }
        }

        public boolean validateObject(final Object obj) {
            if (obj instanceof Channel) {
                final Channel channel = (Channel) obj;
                try {
                	return channel.isOpen();
                } catch (final Exception e) {
                    return false;
                }
            } else {
                return false;
            }
        }

    }
}
