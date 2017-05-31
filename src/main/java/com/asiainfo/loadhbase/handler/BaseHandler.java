package com.asiainfo.loadhbase.handler;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.asiainfo.loadhbase.resource.Record;
import com.asiainfo.loadhbase.tool.FtpTools;
import com.asiainfo.util.ELExplain;

public abstract class BaseHandler {
	protected static final Logger logger = LoggerFactory.getLogger(BaseHandler.class);
	protected static FileSystemXmlApplicationContext appContext;
	protected static Configuration hbaseConfiguration;
	protected static Connection connection;
	protected ELExplain<?> strExplain;

	protected String ftpInfo;
	protected String listFileCmd;
	protected String RemoteType;
	protected String ischgport;
	protected String icfgport;
	protected String recordClassName;
	protected String region;
	protected String tabFamily;
	
	protected int maptotalnum;
	protected String column;
	protected String filterregion;
	
	protected void initProperty() {}

	public abstract void run() throws Exception;

	protected Long GetEveryFiles(List<FileInfo> fileInfoList) throws IllegalStateException {
		String[] ftpInfoList = ftpInfo.split(",");
		Long totalsize = 0l;
		
		for (String ftpInfo : ftpInfoList) {
			String[] ftpdesc = ftpInfo.split(":");
			if (ftpdesc.length != 5) {
				logger.error("ftp config error!" + ftpInfo);
				throw new IllegalStateException();
			}
			
			StringBuffer buf = new StringBuffer();
			buf.append(listFileCmd);
			int index = buf.indexOf("|");
			String cmds = buf.insert(index, ftpdesc[4]).toString();
			FtpTools ftp = FtpTools.newInstance(ftpdesc[0], Integer.valueOf(ftpdesc[1]), ftpdesc[2], ftpdesc[3], ftpdesc[4]);
			try {
				logger.info("ftpInfo->"+ftpdesc[0]+":"+Integer.valueOf(ftpdesc[1])+":"+ftpdesc[4]+",cmds:"+cmds);
				if (ftp.connectServer(Integer.valueOf(RemoteType), Integer.valueOf(icfgport))) {
					totalsize += ftp.getMapList((Record) Class.forName(recordClassName).newInstance(), cmds, fileInfoList);

				} else {
					logger.error("login fail!" + ftpInfo);
					throw new IllegalStateException();
				}
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("FTP error!", e);
				throw new IllegalStateException();
			}
		}
		
		return totalsize;
	}

	public static ApplicationContext getAppContext() {
		return appContext;
	}

	public static void setAppContext(FileSystemXmlApplicationContext appContext) {
		BaseHandler.appContext = appContext;
	}

	public static Configuration getHbaseConfiguration() {
		return hbaseConfiguration;
	}

	public static void setHbaseConfiguration(Configuration hbaseConfiguration) {
		BaseHandler.hbaseConfiguration = hbaseConfiguration;
	}

	public static Connection getConnection() {
		return connection;
	}

	public static void setConnection(Connection connection) {
		BaseHandler.connection = connection;
	}

	public static class FileInfo {
		private Long size = 0L;		//�ļ���С
		private String fileinfo;	//�ļ���Ϣ
		public Long getSize() {
			return size;
		}
		public void setSize(Long size) {
			this.size = size;
		}
		public String getFileinfo() {
			return fileinfo;
		}
		public void setFileinfo(String fileinfo) {
			this.fileinfo = fileinfo;
		}
		
	}

}