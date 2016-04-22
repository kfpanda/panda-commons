package com.kfpanda.mongodb;

import com.kfpanda.util.PropertiesUtil;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonDocument;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 类名： MongoDBDaoImpl
 * 引用: http://www.runoob.com/mongodb/mongodb-java.html
 * 作者： kfpanda
 * 时间： 2014-3-30 下午04:21:11
 */
public class MongoDBDaoImpl implements MongoDBDao {
	private static final Logger logger = LogManager.getLogger(MongoDBDaoImpl.class);

	/**
	 * MongoClient的实例代表数据库连接池，是线程安全的，可以被多线程共享，客户端在多线程条件下仅维持一个实例即可
	 * Mongo是非线程安全的，目前mongodb API中已经建议用MongoClient替代Mongo
	 */
	private MongoClient mongoClient = null;
	private MongoDatabase dbClient = null;
	private static final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	/**
	 * 私有的构造函数
	 */
	private MongoDBDaoImpl() {
		if (mongoClient == null) {
			MongoClientOptions.Builder build = new MongoClientOptions.Builder();
			build.connectionsPerHost(PropertiesUtil.getIntValue("mongo.connectionsPerHost", 20));    //与目标数据库能够建立的最大connection数量为50
			build.threadsAllowedToBlockForConnectionMultiplier(PropertiesUtil.getIntValue("mongo.threadsAllowedToBlockForConnectionMultiplier", 50));    //如果当前所有的connection都在使用中，则每个connection上可以有50个线程排队等待
			/*
			 * 一个线程访问数据库的时候，在成功获取到一个可用数据库连接之前的最长等待时间为2分钟
			 * 这里比较危险，如果超过maxWaitTime都没有获取到这个连接的话，该线程就会抛出Exception
			 * 故这里设置的maxWaitTime应该足够大，以免由于排队线程过多造成的数据库访问失败
			 */
			build.maxWaitTime(PropertiesUtil.getIntValue("mongo.maxWaitTime", 120000));
			build.connectTimeout(PropertiesUtil.getIntValue("mongo.connectTimeout", 10000));    //与数据库建立连接的timeout设置为1分钟
			build.socketKeepAlive(PropertiesUtil.getBooleanValue("mongo.socketKeepAlive"));
			build.socketTimeout(PropertiesUtil.getIntValue("mongo.socketTimeout", 10000));

			MongoClientOptions myOptions = build.build();
			ServerAddress address = new ServerAddress(new InetSocketAddress(PropertiesUtil.getValue("mongo.host", "127.0.0.1"),
					PropertiesUtil.getIntValue("mongo.port", 27017)));

			try {
				//数据库连接实例
				String userName = PropertiesUtil.getValue("mongo.username");
				String password = PropertiesUtil.getValue("mongo.password");
				String dbName = PropertiesUtil.getValue("mongo.dbname");
				if (StringUtils.isNotBlank(userName) && StringUtils.isNotBlank(password)) {
					//配置了用户名 密码
					List<MongoCredential> mongoCredentialList = new ArrayList();
					//MongoCredential.createScramSha1Credential()三个参数分别为 用户名 数据库名称 密码
					MongoCredential credential = MongoCredential.createScramSha1Credential(dbName, userName, password.toCharArray());
					mongoCredentialList.add(credential);
					mongoClient = new MongoClient(address, mongoCredentialList, myOptions);
					dbClient = mongoClient.getDatabase(dbName);
				} else {
					mongoClient = new MongoClient(address, myOptions);
					dbClient = mongoClient.getDatabase(dbName);
				}
			} catch (MongoException e) {
				logger.error("mongo db client create error.", e);
			}
		}
	}

	//类初始化时，自行实例化，饿汉式单例模式
	private static final MongoDBDaoImpl mongoDBDaoImpl = new MongoDBDaoImpl();

	/**
	 * 描述：单例的静态工厂方法
	 * @return MongoDBDaoImpl
	 */
	public static MongoDBDaoImpl getMongoDBDaoImplInstance() {
		return mongoDBDaoImpl;
	}

	@Override
	public MongoDatabase getDatabase(String dbName) {
		return mongoClient.getDatabase(dbName);
	}

	@Override
	public MongoCollection getCollection(String collectionName) {
		return dbClient.getCollection(collectionName);
	}

}
