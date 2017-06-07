package com.asiainfo.loadhbase.tool;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

import com.asiainfo.loadhbase.handler.BaseHandler;
import com.asiainfo.loadhbase.resource.Record;

public class RemoteTools {
	private static final Logger logger = LoggerFactory.getLogger(RemoteTools.class);
	private static Map<String, RemoteTools> ftpClientList = new HashMap<String, RemoteTools>();
	
	private Connection sshConnection; 	// 获取文件列表专用，没缓存信息
	private FTPClient ftpClient;		// 传输文件专用
	private String ip;
	private int    port;
	private String username;
	private String password;
	private String dir;

	private RemoteTools(String ip, int port, String username, String password, String dir) {
		this.ip = ip;
		this.port = port;
		this.username = username;
		this.password = password;
		this.dir = dir;
	}

	public static RemoteTools newInstance(String ip, int port, String username, String password, String dir) {
		RemoteTools ftpTools = ftpClientList.get(new StringBuffer().append(ip).append(port).append(username).append(dir)
				.toString());

		if (ftpTools == null) {
			ftpTools = new RemoteTools(ip, port, username, password, dir);
		}

		return ftpTools;
	}

	// ssh连接仅用于获取文件信息
	public boolean sshConnectServer(int portOfSsh) {
		boolean isAuthenticated = false;

		try {
			sshConnection = new Connection(ip, portOfSsh);
			sshConnection.connect();
			isAuthenticated = sshConnection.authenticateWithPassword(username, password);
		} catch (IOException e) {
			logger.info("sshconnect exception:", e.getMessage());
		}
		return isAuthenticated;
	}

	// ssh获取文件列表，并校验文件名
	public long sshGetFileInfoList(Record base, String cmd, List<BaseHandler.FileInfo> fileinfos) {
		List<String> files = sshSubFileInfoList(cmd);
		long totalsize = 0l;
		int linenum = 0;

		try {
			for (String fname : files) {
				linenum++;
				if (linenum == 1) { // 第一行非文件列表过滤
					continue;
				}
				String[] fnames = fname.split("\\|");

				if (base.checkFileName(fnames[1])) {
					BaseHandler.FileInfo fileinfo = new BaseHandler.FileInfo();
					fileinfo.setSize(Long.valueOf(fnames[0]));
					fileinfo.setFileinfo(new StringBuilder().append(this.ip).append(":").append(this.port).append(":")
							.append(this.username).append(":").append(this.password).append(":").append(this.dir)
							.append(":").append(fnames[1]).append(":").append(fnames[0]).toString());
					fileinfos.add(fileinfo);
					totalsize += Long.valueOf(fnames[0]);
				}
			}
		} catch (NumberFormatException e) {
			logger.info("getMapList:" + e.getMessage());
		}
		return totalsize;
	}

	private List<String> sshSubFileInfoList(String cmd) {
		List<String> flist = null;
		try {
			if (null == sshConnection) {
				throw new ConnectException();
			}
			
			Session sess = sshConnection.openSession();
			sess.execCommand(cmd);
			InputStream stdout = new StreamGobbler(sess.getStdout());
			BufferedReader br = new BufferedReader(new InputStreamReader(stdout));
			flist = new ArrayList<String>();
			String line = br.readLine();
			while (null != line) {
				flist.add(line);
				line = br.readLine();
			}

			if (null != stdout) {
				stdout.close();
			}

			if (null != br) {
				br.close();
			}

			if (null != sess) {
				sess.close();
			}

			if (null != sshConnection) {
				sshConnection.close();
			}

		} catch (IOException e) {
			e.printStackTrace(System.err);
			return flist;
		}

		return flist;
	}

	// ftp的连接
	public boolean ftpConnectServer() throws SocketException, IOException {
		boolean flag = false;
		logger.info(this.toString());

		if (!isConnected()) {
			ftpClient = new FTPClient();
			ftpClient.connect(ip, port);
			ftpClient.setDefaultTimeout(Integer.MAX_VALUE);
			ftpClient.setControlEncoding("GBK");
			FTPClientConfig conf = new FTPClientConfig(FTPClientConfig.SYST_NT);
			conf.setServerLanguageCode("zh");

			if (FTPReply.isPositiveCompletion(ftpClient.getReplyCode())) {
				if (ftpClient.login(username, password)) {
					if (dir != null && dir.length() != 0) {
						boolean bchg = ftpClient.changeWorkingDirectory(dir);
						logger.info("chgdir:" + ftpClient.printWorkingDirectory() + " ischg:" + bchg);
						ftpClientList.put(new StringBuffer().append(this.ip).append(this.port).append(this.username)
								.append(this.dir).toString(), this);
						flag = true;
					}
				} else {
					logger.info("ftpClient.disconnect.login fail:" + username + "/" + password);
					ftpClient.disconnect();
				}
			}
		} else {
			flag = true;
		}

		return flag;
	}

	public boolean downLoad2Stream(String filename, OutputStream bos) throws UnsupportedEncodingException, IOException {
		ftpClient.enterLocalPassiveMode();
		ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
		logger.info("downLoad2Stream filename:" + filename);
		boolean bsuccess = this.ftpClient.retrieveFile(new String(filename.getBytes("GBK"), "iso-8859-1"), bos);
		bos.flush();
		logger.info("flush success");

		return bsuccess;
	}

	public byte[] downLoad2Buf(String filename) throws UnsupportedEncodingException, IOException {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		BufferedOutputStream bos = new BufferedOutputStream(bout);
		ftpClient.enterLocalPassiveMode();
		byte[] ctx;
		ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
		this.ftpClient.retrieveFile(new String(filename.getBytes("GBK"), "iso-8859-1"), bos);
		logger.info("downLoad2Buf filename:" + filename);
		bos.flush();
		logger.info("flush");
		ctx = bout.toByteArray();
		bos.close();
		bout.close();

		return ctx;
	}
	
	public boolean writeFile(InputStream is, String detaildir, String fileName) throws IOException {
		if (null == is) {
			return false;
		}

		// 指定写入的目录
		if (ftpClient.changeWorkingDirectory(detaildir)
				|| (ftpClient.makeDirectory(detaildir) && ftpClient.changeWorkingDirectory(detaildir))) {
			// 写操作
			ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
			FTPFile[] files = ftpClient.listFiles(fileName);
			if (files.length >= 1) {
				Long size = files[0].getSize();
				ftpClient.setRestartOffset(size);
			}

			boolean bReturn = ftpClient.storeFile(new String(fileName.getBytes("GBK"), "iso-8859-1"), is);
			ftpClient.changeWorkingDirectory(dir);

			return bReturn;

		} else {
			return false;
		}
	}
	
	public boolean delete(String filename) throws IOException {
		return this.ftpClient.deleteFile(new String(filename.getBytes("GBK"), "iso-8859-1"));
	}

	public boolean rename(String fromfilename, String tofilename) throws IOException {
		return this.ftpClient.rename(fromfilename, tofilename);
	}

	public String toString() {
		return "ip=" + ip + ",port=" + port + ",username=" + username + ",password=" + password + ",dir=" + dir;
	}

	public boolean isConnected() {
		return ftpClient != null && ftpClient.isConnected();
	}

	public void disConnect() throws IOException {
		this.ftpClient.disconnect();
	}

	public FTPClient getFtpClient() {
		return ftpClient;
	}

	public void setFtpClient(FTPClient ftpClient) {
		this.ftpClient = ftpClient;
	}

	public static Map<String, RemoteTools> getFtpClientList() {
		return ftpClientList;
	}

	public static void setFtpClientList(Map<String, RemoteTools> ftpClientList) {
		RemoteTools.ftpClientList = ftpClientList;
	}

}
