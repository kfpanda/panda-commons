package com.kfpanda.mq.pool;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool.Config;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitmqPool extends Pool<Connection>{
	private static final int DEFAULT_PORT = 5672;
	
	public RabbitmqPool(String host) {
        super(new Config(), new RabbitmqFactory(host, DEFAULT_PORT,
                null, 0, null, null));
    }
	
    public RabbitmqPool(String host, int port) {
        super(new Config(), new RabbitmqFactory(host, port,
                null, 0, null, null));
    }

    public RabbitmqPool(final Config poolConfig, final String host, int port,
    		final String virtualHost, int timeout, final String userName, final String password) {
        super(poolConfig, new RabbitmqFactory(host, port, virtualHost, timeout, userName, password));
    }

    public RabbitmqPool(final GenericObjectPool.Config poolConfig,
            final String host, final int port) {
        this(poolConfig, host, port, null, 0, null, null);
    }
    
    public RabbitmqPool(final GenericObjectPool.Config poolConfig,
            final String host, final int port, final String virtualHost ) {
        this(poolConfig, host, port, virtualHost, 0, null, null);
    }

    public RabbitmqPool(final GenericObjectPool.Config poolConfig,
            final String host, final int port, final int timeout) {
        this(poolConfig, host, port, null, timeout, null, null);
    }

    /**
     * PoolableObjectFactory custom impl.
     */
    private static class RabbitmqFactory extends BasePoolableObjectFactory {
    	private final ConnectionFactory factory = new ConnectionFactory();
        private final String host;
        private final int port;
        private final String virtualHost;
        private final int timeout;
        private final String userName;
        private final String password;
        
        public RabbitmqFactory(final String host, final int port, final String virtualHost,
                final int timeout, final String userName, final String password) {
            super();
            this.host = host;
            this.port = port;
            this.virtualHost = virtualHost;
            this.timeout = (timeout > 0) ? timeout : -1;
            this.userName = userName;
            this.password = password;
        }

        public Object makeObject() throws Exception {
            final Connection conn;
            //最大channel的创建数为30个
            factory.setRequestedChannelMax(30);
            factory.setHost(this.host);
            factory.setPort(this.port);
            if(userName != null && userName != ""){
            	factory.setUsername(userName);
            }
            if(password != null && password != ""){
            	factory.setPassword(password);
            }
            factory.setAutomaticRecoveryEnabled(true);
            if(this.timeout > 0){
            	factory.setConnectionTimeout(this.timeout);
            }
            if(virtualHost != null && virtualHost != ""){
            	factory.setVirtualHost(virtualHost);
            }
            conn = factory.newConnection();
            return conn;
        }

        public void destroyObject(final Object obj) throws Exception {
            if (obj instanceof Connection) {
                final Connection conn = (Connection) obj;
                if(conn.isOpen()){
                	conn.close();
                }
            }
        }

        public boolean validateObject(final Object obj) {
            if (obj instanceof Connection) {
                final Connection conn = (Connection) obj;
                try {
                	return conn.isOpen();
                } catch (final Exception e) {
                    return false;
                }
            } else {
                return false;
            }
        }

    }
}
