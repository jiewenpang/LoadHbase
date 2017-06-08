package com.asiainfo.loadhbase.handler;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.net.ftp.FTPFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asiainfo.loadhbase.resource.Record;
import com.asiainfo.loadhbase.tool.RemoteTools;
import com.asiainfo.loadhbase.tool.LCompress;

public abstract class BaseHandler {
	protected static final Logger logger = LoggerFactory.getLogger(BaseHandler.class);
	protected static Configuration hbaseConfiguration;
	protected static Connection connection;
	protected static FileSystem fileSystem;

	protected String name;
	protected String ftpInfo;
	protected String listFileCmd;
	protected String remoteType;
	protected String portOfSsh;
	protected String inputHdfsPath;
	protected String maxFileSize;
	protected String maxFileHandlePath;
	protected String inputBakPath;
	protected String detailOutputPath;
	protected String detailOutputFileName;
	protected Record record;

	protected void initProperty() {
		if (BaseHandler.fileSystem == null) {
			throw new IllegalStateException();
		}
	}

	public abstract void run() throws Exception;

	protected Long GetEveryFileInfo(List<FileInfo> fileInfoList) throws IllegalStateException {
		String[] ftpInfoList = ftpInfo.split(",");
		long totalsize = 0l;
		
		for (String ftpInfo : ftpInfoList) {
			String[] ftpdesc = ftpInfo.split(":");
			if (ftpdesc.length != 5) {
				logger.error("rmtTools config error:" + ftpInfo);
				throw new IllegalStateException();
			}
			
			/*
			 * 命令：ls -lrt | awk '{print $5"|"$9}' |head -15000
			 * ftpInfo样例：192.252.106.49:21:qzd_qd:Audi@369:/data02/cxbill
			 * FileInfo样例：192.252.106.50:21:qzd_qd:Audi@369:/data02/HZ_boss_211:boss.11.1130511512.20.0.201706.20170606.GZ0Q.ZHZW.Z:14859
			 */
			StringBuffer buf = new StringBuffer(listFileCmd);
			String cmds = buf.insert(buf.indexOf("|"), ftpdesc[4]).toString();
			RemoteTools rmtTools = RemoteTools.newInstance(ftpdesc[0], Integer.valueOf(ftpdesc[1]), ftpdesc[2], ftpdesc[3], ftpdesc[4]);
			
			try {
				logger.info("ftpInfo:"+Arrays.toString(ftpdesc) + ",cmds:["+cmds+"]");
				if (rmtTools.sshConnectServer(Integer.valueOf(portOfSsh))) {
					totalsize += rmtTools.sshGetFileInfoList(record, cmds, fileInfoList);
				} else {
					logger.error("login fail!");
					throw new IllegalStateException();
				}
			} catch (Exception e) {
				logger.error("FTP error!", e);
				throw new IllegalStateException();
			}
		}
		
		return totalsize;
	}

	protected static void NormalCleanUp(Record record, String maxFileHandlePath, String detailOutputPath, 
			String detailOutputFileName) throws IOException {
		logger.info("NormalCleanUp:" + RemoteTools.getFtpClientList().size());

		// 移走明细文件，并断开ftp
		RemoteTools rmtTools = null;
		Iterator<Entry<String, RemoteTools>> it = RemoteTools.getFtpClientList().entrySet().iterator();
		while (it.hasNext()) {
			try {
				rmtTools = it.next().getValue();
				if (rmtTools.getFtpClient().changeWorkingDirectory(detailOutputPath)) {
					FTPFile[] files = rmtTools.getFtpClient().listFiles(detailOutputFileName);
					if (files.length >= 1) {
						logger.info("rename detailfile:" + detailOutputFileName + ",result:" 
							+ rmtTools.rename(detailOutputFileName, detailOutputFileName.substring(0, detailOutputFileName.length() - 4)));
					}
				}

				if (rmtTools.isConnected()) {
					rmtTools.disConnect();
				}
			} catch (Exception e) {
				logger.warn("close rmtTools exception:", e);
			}
		}
		RemoteTools.getFtpClientList().clear();

		// 刷新并关闭表
		if (!record.getMapTable().isEmpty()) {
			Iterator<Entry<String, Table>> it1 = record.getMapTable().entrySet().iterator();
			while (it1.hasNext()) {
				Entry<String, Table> entry = it1.next();
				Table table = entry.getValue();
				table.close(); // 是否要先flush？
			}
		}
		record.getMapTable().clear();
	}

	protected static void NormalProcessOneFile(Configuration conf, Record record, String[] fileInfo, String maxFileHandlePath, 
			Long maxFileSize, String detailOutputPath, String inputBakPath, String detailOutputFileName, String jobName) {
		if (fileInfo.length != 6) {
			logger.error("FtpInfo config error, continue:" + Arrays.toString(fileInfo));
			return;
		}
		
		final String CHARTSET = "GBK";
		RemoteTools rmtTools = RemoteTools.newInstance(fileInfo[0], Integer.valueOf(fileInfo[1]), fileInfo[2], fileInfo[3], fileInfo[4]);
		int linenum = 0;// 文件总行数,无文件头
		int inputlinenum = 0;// 实际入库行数
		try {
			if (rmtTools.ftpConnectServer()) {
				logger.info("connect success!");
				ByteArrayInputStream bin = null;
				FSDataInputStream inStream = null;
				BufferedReader br = null;
				// 获取原文件的行数
				if (maxFileSize < Long.valueOf(fileInfo[6])) { // 大文件特殊处理
					FileSystem fileSystem = FileSystem.get(conf);
					Path datapath = new Path(maxFileHandlePath + "/" + fileInfo[5]);
					FSDataOutputStream out = fileSystem.create(datapath);
					rmtTools.downLoad2Stream(fileInfo[5], out);
					out.close();
					inStream = fileSystem.open(datapath);
					br = new BufferedReader(new InputStreamReader(inStream, CHARTSET));
				} else {
					byte[] bos = rmtTools.downLoad2Buf(fileInfo[5]);
					logger.info("downLoad2Buf finish!");
					if (bos.length == 0) {
						logger.info(fileInfo[5] + "file is empty");
						return;
					}
					if (fileInfo[5].toLowerCase().endsWith(".z")) {// 解压
						logger.info("rmtTools:deCompress");
						bos = LCompress.deCompress(bos);
						logger.info("rmtTools:deCompress finish");
					}
					bin = new ByteArrayInputStream(bos);
					br = new BufferedReader(new InputStreamReader(bin, CHARTSET));
				}

				long starttime = System.currentTimeMillis();
				logger.info("start DealFile:" + fileInfo[5]);
				// 业务处理
				linenum = record.buildRecord(fileInfo[5], br, connection);
				logger.info("insert Hbase Finish!recodeCount:" + linenum + ",Time Consuming:" + (System.currentTimeMillis() - starttime) + "ms.");
				logger.info(fileInfo[5] + ":process" + linenum);
				RecursionDeleleFile(rmtTools, fileInfo, 3, linenum, inputlinenum, detailOutputPath, inputBakPath, detailOutputFileName, jobName);
			} else {
				logger.info("rmtTools error!");
			}
		} catch (SocketException e) {
			logger.error("map SocketException:" + e.getMessage());
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			logger.error("map UnsupportedEncodingException:" + e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			logger.error("map IOException:", e);
		} catch (Exception e) {
			logger.error("", e);
		}
	}

	private static void RecursionDeleleFile(RemoteTools rmtTools, String[] fileInfo, int times, int linenum,
			int inputlinenum, String detailOutputPath, String inputBakPath, String detailOutputFileName, String jobName) {
		if (times <= 0) {
			logger.error("recursiondelfile fail filename:" + fileInfo[5]);
		} else {
			try {
				if (rmtTools.ftpConnectServer()) {
					if (detailOutputPath!=null && !detailOutputPath.equals("")) {
						BakDetailFile(fileInfo, rmtTools, linenum, inputlinenum, detailOutputPath, detailOutputFileName, jobName);
					}
					if (inputBakPath==null || inputBakPath.contains("")) {
						logger.info("delete file:" + fileInfo[5] + "," + rmtTools.delete(fileInfo[5]));
					} else {
						String tofiledir = getTofilename(fileInfo[4], inputBakPath);
						rmtTools.rename(fileInfo[4] + "/" + fileInfo[5], tofiledir + "/" + fileInfo[5]);
					}
				} else {
					logger.warn("rmtTools login fail!");
				}
			} catch (IOException e) {
				times = times - 1;
				logger.warn("recursiondelfile IOException:" + e.getMessage());
				e.printStackTrace();
				RecursionDeleleFile(rmtTools, fileInfo, times, linenum, inputlinenum, detailOutputPath, inputBakPath, 
						detailOutputFileName, jobName);
			}
		}
	}

	// rmtTools 接口机备份 处理过的文件
	private static void BakDetailFile(String[] fileInfo, RemoteTools rmtTools, int linenum, int inputlinenum, 
			String detailOutputPath, String detailOutputFileName, String jobName) throws UnknownHostException, IOException {

		// 文件名:JobId_当前处理map主机名_当前处理主机map进程号_yyyymmddhh24miss
		if ("".equals(detailOutputFileName)) {
			String hostname = InetAddress.getLocalHost().getHostName();
			String runtime = ManagementFactory.getRuntimeMXBean().getName();
			Integer processnum = Integer.parseInt(runtime.substring(0, runtime.indexOf("@")));
			String date = new SimpleDateFormat("yyyyMMddHH24mmss").format(new Date());
			detailOutputFileName = jobName + "_" + hostname + "_" + processnum + "_" + date + ".tmp";
		}
		
		// 文件内容:主机地址|文件名|文件大小|文件记录数|
		String content = fileInfo[0] + "|" + fileInfo[4] + "/" + fileInfo[5] + "|" + fileInfo[6] + "|" + linenum + "|"
				+ inputlinenum + "\n";
		InputStream is = new ByteArrayInputStream(content.getBytes());
		boolean flag = rmtTools.writeFile(is, detailOutputPath, detailOutputFileName);
		is.close();
		if (!flag) {
			throw new IOException("write detail file isSuccess:" + flag);
		}
	}

	private static String getTofilename(String dir, String inputBakPath) {
		int idx = dir.lastIndexOf("/");
		if (dir.length() == idx + 1)
			idx = dir.lastIndexOf("/", idx - 1);

		String tofiledir = dir.substring(0, idx) + "/" + inputBakPath;
		return tofiledir;
	}
	
	public static Configuration getHbaseConfiguration() {
		return hbaseConfiguration;
	}

	public static void setHbaseConfiguration(Configuration hbaseConfiguration) {
		BaseHandler.hbaseConfiguration = hbaseConfiguration;
		try {
			BaseHandler.fileSystem = FileSystem.get(hbaseConfiguration);
		} catch (IOException e) {
			logger.warn("init hdfs fileSystem fail", e);
		}
	}

	public static Connection getConnection() {
		return connection;
	}

	public static void setConnection(Connection connection) {
		BaseHandler.connection = connection;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getFtpInfo() {
		return ftpInfo;
	}

	public void setFtpInfo(String ftpInfo) {
		this.ftpInfo = ftpInfo;
	}

	public String getListFileCmd() {
		return listFileCmd;
	}

	public void setListFileCmd(String listFileCmd) {
		this.listFileCmd = listFileCmd;
	}

	public String getRemoteType() {
		return remoteType;
	}

	public void setRemoteType(String remoteType) {
		this.remoteType = remoteType;
	}

	public String getPortOfSsh() {
		return portOfSsh;
	}

	public void setPortOfSsh(String portOfSsh) {
		this.portOfSsh = portOfSsh;
	}
	
	public String getInputHdfsPath() {
		return inputHdfsPath;
	}

	public void setInputHdfsPath(String inputHdfsPath) {
		this.inputHdfsPath = inputHdfsPath;
	}

	public String getMaxFileSize() {
		return maxFileSize;
	}

	public void setMaxFileSize(String maxFileSize) {
		this.maxFileSize = maxFileSize;
	}

	public String getMaxFileHandlePath() {
		return maxFileHandlePath;
	}

	public void setMaxFileHandlePath(String maxFileHandlePath) {
		this.maxFileHandlePath = maxFileHandlePath;
	}

	public String getInputBakPath() {
		return inputBakPath;
	}

	public void setInputBakPath(String inputBakPath) {
		this.inputBakPath = inputBakPath;
	}

	public String getDetailOutputPath() {
		return detailOutputPath;
	}

	public void setDetailOutputPath(String detailOutputPath) {
		this.detailOutputPath = detailOutputPath;
	}

	public String getDetailOutputFileName() {
		return detailOutputFileName;
	}

	public void setDetailOutputFileName(String detailOutputFileName) {
		this.detailOutputFileName = detailOutputFileName;
	}

	public Record getRecord() {
		return record;
	}

	public void setRecord(Record record) {
		this.record = record;
	}

	public static class FileInfo {
		private Long size = 0L;		//文件大小
		private String fileinfo;	//文件信息
		
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