package com.kfpanda.mongodb;

import com.mongodb.DBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.util.Map;

/**
* 类名： MongoDBDao
* 作者： kfpanda
* 时间： 2014-8-30 下午03:46:55
* 描述： 
*/
public interface MongoDBDao {
	/**
	 * 描述：获取指定的mongodb数据库
	 * @param dbName
	 * @return
	 */
	MongoDatabase getDatabase(String dbName);

	/**
	 * 描述：获取指定mongodb数据库的collection集合
	 * @param collectionName	数据库集合
	 * @return
	 */
	MongoCollection getCollection(String collectionName);

}

