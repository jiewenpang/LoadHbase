package com.asiainfo.loadhbase;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.User;

import com.asiainfo.util.HbaseLoginUtil;

public class Constant {
	private static Configuration hadoopConfig;
	private static Connection connection = null;
	private static HashMap<String, String> propertyMap;
	public static String USERNAME = "HBaseDeveloper";
	public static String USERKEYTABFILE = "./user.keytab";
	public static String KRB5FILE = "./krb5.conf";
	private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
	private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
	private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop";

	public void initLog() throws IOException {
		System.setProperty("LOG_HOME", propertyMap.get("LOG_PATH"));

		File logbackConfFile = new File(propertyMap.get("LOGBACK_CONF_FILE"));
		if (!logbackConfFile.canRead()) {
			System.out.println("日志配置文件logback.xml不存在:" + logbackConfFile.getAbsolutePath());
			throw new IllegalStateException();
		}

		if (System.getProperty("logback.configurationFile") == null) {
			System.setProperty("logback.configurationFile", logbackConfFile.getAbsolutePath());
		}
	}

	public void initHbase() throws IOException {
		hadoopConfig = HBaseConfiguration.create();

		try {
			connection = ConnectionFactory.createConnection(hadoopConfig);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		Constant.USERNAME = propertyMap.get("USERNAME");
		Constant.USERKEYTABFILE = propertyMap.get("USERKEYTABFILE");
		Constant.KRB5FILE = propertyMap.get("KRB5FILE");
		// 如果是安全集群，则在登录之前需要验证
		if (User.isHBaseSecurityEnabled(hadoopConfig)) {

			try {
				// 设置登录的上下文名字为client
				HbaseLoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, Constant.USERNAME, Constant.USERKEYTABFILE);
				
				// 设置登录的登录SERVER_PRINCIPAL_KEY和DEFAULT_SERVER_PRINCIPAL
				HbaseLoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);

				// 设置user.keytab和krb5.conf
				HbaseLoginUtil.login(Constant.USERNAME, Constant.USERKEYTABFILE, Constant.KRB5FILE, hadoopConfig);
				
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}
	
	public void setPropertyMap(HashMap<String, String> propertyMap) {
		Constant.propertyMap = propertyMap;
	}

	public static Connection getConnection() {
		return connection;
	}

	public static void setConnection(Connection connection) {
		Constant.connection = connection;
	}

}
