package com.kfpanda.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 单例模式 读取配置文件的相关信息 
 * 该配置文件有3中读取方式 :
 * 		1.具体的文件路径
 * 		2.相对类路径的文件路径 
 * 		3.远程URL路径
 * @author kfpanda 2015-7-14 上午10:55:45
 */
public class PropertiesUtil {
	
	private static PropertiesUtil instance = new PropertiesUtil();
	
	private static Properties config = null;
	
	private static String configPath = null;
	
	private static Logger LOG = LoggerFactory.getLogger(PropertiesUtil.class);
	
	
	public static void init() {
		LOG.info(">>> loading the properties .....");
		String confPath = System.getProperty("application.config");
		if(confPath == null){
		    confPath = "properties/application.properties";
		}
		getInstance(confPath);
		
		LOG.info(">>> loaded the properties ... ... ... ... ... ... ... ... OK!");
	}
	
	public static PropertiesUtil getInstance(String configpath) {
		if (configPath == null || (!configPath.equalsIgnoreCase(configpath))){
			configPath = configpath;
			loadProperties();
		}
			
		return getInstance();
	}
	
	public static PropertiesUtil getInstance() {
		return instance;
	}
	
	public static Properties getConfig() {
        return config;
    }

	public String getValue(String name, String defaultValue) {
		return config.getProperty(name, defaultValue);
	}

	public String getValue(String name) {
		return getValue(name, null);
	}
	
	public Integer getIntValue(String name) {
		return Integer.parseInt(getValue(name, null));
	}
	
	public Boolean getBooleanValue(String name) {
		return Boolean.parseBoolean(getValue(name, null));
	}

	/**
	 * 参数configpath可以 是远程URL 也可以是相对classpath的路径 还可以是绝对路径
	 * 
	 * @param configpath
	 */
	private static void loadProperties() {
		if (configPath == null) {
			LOG.error("文件的路径不能为空");
			return;
		}

		InputStream ins = null;
		
		try {
			ins = new FileInputStream(configPath);
		} catch (FileNotFoundException e) {
			LOG.debug("绝对路径:" + configPath + " 找不到该文件");
		}

		if (ins == null) {
			try {
				ins = new FileInputStream(FilePath.getAbsolutePathWithClass() + configPath);
			} catch (FileNotFoundException e) {
				LOG.debug("classpath 路径下, " + configPath + " 文件无法找到.");
			}
		}

		if (ins == null) {
			LOG.debug("类路径下:" + configPath + " 找不到该文件");
			try {
				ins = (new URL(configPath)).openStream();
			} catch (Exception e) {
				LOG.debug("远程路径:" + configPath + " 取不到该文件");
			}
		}

		try {
			if (ins != null) {
				config = new Properties();
				config.load(ins);
			}
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		} finally {
			if (ins != null) {
				try {
					ins.close();
				} catch (IOException e) {
					LOG.error(e.getMessage(), e);
				}
			}
		}
		
		if(ins == null){
			LOG.error("加载属性文件失败");
		}
	}
}
