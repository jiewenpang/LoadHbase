package com.asiainfo.loadhbase.tool;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.asiainfo.loadhbase.handler.BaseHandler;
import com.asiainfo.loadhbase.resource.Record;


public class FtpTools {
	static final Log LOG = LogFactory.getLog(FtpTools.class);
	
	private String ip;
	private int port;
	private String username;
	private String password;
	private String dir;
	private FTPClient ftpClient;
	private static SSHClient sshClient;
	
	public FTPClient getFtpClient() {
		return ftpClient;
	}
	public void setFtpClient(FTPClient ftpClient) {
		this.ftpClient = ftpClient;
	}

	public static Map<String,FtpTools> ftpClientList=new HashMap<String,FtpTools>();
	
	public FtpTools(String ip,int port,String username,String password,String dir) {
		this.ip=ip;
		this.port=port;
		this.username=username;
		this.password=password;
		this.dir=dir;
	}
	/**
	 * 初始化
	 * @param ip
	 * @param port
	 * @param username
	 * @param password
	 * @param dir
	 */
	public static FtpTools newInstance(String ip,int port,String username,String password,String dir) {
		FtpTools f=ftpClientList.get(new StringBuffer().append(ip).append(port).append(username).append(dir).toString());
		if(f==null){
			f= new FtpTools(ip,port,username,password,dir);
		}
		
		return f;
	}
	
	public boolean connectServer()  throws SocketException, IOException{
		boolean flag=false;
		LOG.info("ip="+ip+",port="+port+",username="+username+",password="+password);
//		try {
			if(!this.isConned()){
				ftpClient=new FTPClient();
				ftpClient.connect(ip,port);
				ftpClient.setDefaultTimeout(Integer.MAX_VALUE);
				ftpClient.setControlEncoding("GBK");
				FTPClientConfig conf = new FTPClientConfig(FTPClientConfig.SYST_NT);  
				conf.setServerLanguageCode("zh");
				
				if (FTPReply.isPositiveCompletion(ftpClient.getReplyCode())) {
					if (ftpClient.login(username, password)) {
						if (dir!=null && dir.length() != 0){
							boolean bchg = ftpClient.changeWorkingDirectory(dir);
							LOG.info("chgdir:" + ftpClient.printWorkingDirectory()+" ischg:"+ bchg);
							ftpClientList.put(new StringBuffer().append(this.ip).append(this.port).append(this.username).append(this.dir).toString(),this);
							flag=true;
						}
					}else{
						LOG.info("ftpClient.disconnect.login fail:" + username + "/" + password);
						ftpClient.disconnect();
					}
				}
			}else{
				flag=true;
			}
/*		} catch (SocketException e) {
			// TODO Auto-generated catch block
			LOG.info("SocketException" + e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.info("IOException" + e.getMessage());
			e.printStackTrace();
			
		}*/
		return flag;
	}
	/**
	 * 连接
	 * @return
	 * @throws Exception
	 */
	public boolean connectServer(int itype, int iport) throws Exception {
		if(itype == 1) {
			return ftpconnect();
		} else {
			return sshconnect(iport);
		}
	}
	
	private boolean sshconnect(int iport) {
		// TODO Auto-generated method stub
		
		boolean flag = false;
		try {
			sshClient = SSHClient.NewInstance(ip, iport, username, password);
			if(null == sshClient){
				throw new IllegalArgumentException("sshClient not null!");
			}
			flag = sshClient.getConnection();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.info("sshconnect exception:" + e.getMessage());
			e.printStackTrace();
		}
		return flag;
	}
	private boolean ftpconnect() {
		// TODO Auto-generated method stub
		boolean flag=false;
		try {
			if(!this.isConned()){
				ftpClient=new FTPClient();
				ftpClient.connect(ip,port);
				ftpClient.setControlEncoding("GBK");
				FTPClientConfig conf = new FTPClientConfig(FTPClientConfig.SYST_NT);  
				conf.setServerLanguageCode("zh");  
				if (FTPReply.isPositiveCompletion(ftpClient.getReplyCode())) {
					if (ftpClient.login(username, password)) {
						if (dir!=null && dir.length() != 0){
							ftpClient.changeWorkingDirectory(dir);
							ftpClientList.put(new StringBuffer().append(this.ip).append(this.port).append(this.username).append(this.dir).toString(),this);
							flag=true;
						}
					}else{
						ftpClient.disconnect();
					}
				}
			}else{
				flag=true;
			}
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			LOG.info("ftpconnect SocketException:" + e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.info("ftpconnect IOException:" + e.getMessage());
			e.printStackTrace();
		}
		return flag;
		
	}
/*	public boolean connectServer(Content content) throws Exception {
		
		SSHClient sshClient = SSHClient.NewInstance(content);
		conn = sshClient.getConnection(content);
		 Now connect 
		 Authenticate.
		 * If you get an IOException saying something like
		 * "Authentication method password not supported by the server at this stage."
		 * then please check the FAQ.
		 
		
		boolean isAuthenticated = conn.authenticateWithPassword(content.getUsername(), content.getPassword());
		
		if (isAuthenticated == false)
			throw new IOException("Authentication failed.");
		
		
		return conn.authenticateWithPassword(content.getUsername(), content.getPassword());
	}*/
	/**
	 * 文件列表
	 * @return
	 * @throws Exception
	 */
	public List<String> getFileList() throws IOException{
		FTPFile[] ftpFiles = ftpClient.listFiles();
		List<String> flist = new ArrayList<String>();
		if(ftpFiles!=null && ftpFiles.length>0){
			for(FTPFile f:ftpFiles)flist.add(f.getName());
		}
		return flist;
	}
	
/*	public List<String> getFileList(String hostname, String username, String password, String cmd){
		SSHClient sftp = SSHClient.NewInstance(hostname, port, username, password, cmd);
		return sftp.getFileList();
		
	}*/
	
	public List<String> getMapList(Record base){
		List<String> result=new ArrayList<String>();
		try {
			List<String> files = this.getFileList();
			for(String fname:files){
				if(base.checkFileName(fname))
					result.add(new StringBuilder().append(this.ip).append(":").append(this.port).append(":").append(this.username).append(":").append(this.password).append(":").append(this.dir).append(":").append(fname).toString());
			}
		} catch (Exception e) {
			//
			LOG.info("getMapList:"+e.getMessage());
		}
		return result;
	}
	//ssh获取文件列表
	public List<String> getMapList(Record base, String cmd){
		List<String> result = new ArrayList<String>();
		try {
			List<String> files = sshClient.getFileList(cmd);
//			List<String> files = this.getFileList(content.getHostname(), content.getUsername(), content.getPassword(), content.getCmd());
			for(String fname : files){
				if(base.checkFileName(fname))
					result.add(new StringBuilder().append(this.ip).append(":").append(this.port).append(":").append(this.username).append(":").append(this.password).append(":").append(this.dir).append(":").append(fname).toString());
			}
		} catch (Exception e) {
			LOG.info("getMapList:" + e.getMessage());
		}
		return result;
	}
	//ssh获取文件列表和各个文件大小
	public Long getMapList(Record base, String cmd, List<BaseHandler.FileInfo> fileinfos){
		List<String> files = sshClient.getFileList(cmd);
		Long totalsize = 0l;
		int linenum = 0;
		try {
			for(String fname : files){
				linenum ++;
				if(linenum == 1){	//第一行非文件列表过滤
					continue;
				}
				String[] fnames = fname.split("\\|");
				totalsize += Long.valueOf(fnames[0]);
				
				if(base.checkFileName(fnames[1])){
					BaseHandler.FileInfo fileinfo = new BaseHandler.FileInfo();
					fileinfo.setSize(Long.valueOf(fnames[0]));
					fileinfo.setFileinfo(new StringBuilder().append(this.ip).append(":").append(this.port).append(":").append(this.username).append(":").append(this.password).append(":").append(this.dir).append(":").append(fnames[1]).append(":").append(fnames[0]).toString());
					fileinfos.add(fileinfo);
				}
			}
		} catch (NumberFormatException e) {
			LOG.info("getMapList:" + e.getMessage());
		}
		return totalsize;
	}
	
	/**
	 * 下载到内存
	 * @param filename
	 * @return
	 * @throws Exception
	 */
	public byte[] download2Buf(String filename) throws UnsupportedEncodingException, IOException{
		ByteArrayOutputStream bout=new ByteArrayOutputStream();
		BufferedOutputStream bos=new BufferedOutputStream(bout);
		ftpClient.enterLocalPassiveMode();
		byte[] ctx;
		ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
		this.ftpClient.retrieveFile(new String(filename.getBytes("GBK"), "iso-8859-1"), bos);
		LOG.info("download2Buf filename:" + filename);
		bos.flush();
		LOG.info("flush");
		ctx = bout.toByteArray();
		bos.close();
		bout.close();

		return ctx;
	}
	
	/**
	 * 下载到文件
	 * @param filename
	 * @return flag,true--success,false--fail  
	 * @throws Exception
	 */
	public boolean download(String filename, OutputStream bos)throws UnsupportedEncodingException, IOException{
		ftpClient.enterLocalPassiveMode();
		ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
		LOG.info("download filename:" + filename);
		boolean bsuccess = this.ftpClient.retrieveFile(new String(filename.getBytes("GBK"), "iso-8859-1"), bos);
		bos.flush();
		LOG.info("flush success");
		
		return bsuccess;
	}
	
	/**
	 * 删除
	 * @param filename
	 * @return
	 * @throws Exception
	 */
	public boolean delete(String filename)throws IOException{
		return this.ftpClient.deleteFile(new String(filename.getBytes("GBK"), "iso-8859-1"));
	}
	
	/**
	 * 重命名
	 * @param fromfilename
	 * @param tofilename
	 * @return
	 * @throws IOException 
	 */
	public boolean rename(String fromfilename, String tofilename) throws IOException{
		return this.ftpClient.rename(fromfilename, tofilename);
	}
	
	public boolean isConned(){
		return ftpClient!=null && ftpClient.isConnected();
	}
	
	public void disConnect() throws IOException{
		this.ftpClient.disconnect();
	}
	
	/**
	 * ftp 文件续传
	 * */
	public boolean writeFile(InputStream is, String detaildir, String fileName) throws IOException{
		
		if(null == is){
			return false;
		}
         // 指定写入的目录  
		if(ftpClient.changeWorkingDirectory(detaildir) || (ftpClient.makeDirectory(detaildir) && ftpClient.changeWorkingDirectory(detaildir))) {
			// 写操作 
			ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
			FTPFile[] files = ftpClient.listFiles(fileName);
			if(files.length>=1){
				Long size = files[0].getSize();
				ftpClient.setRestartOffset(size); 
			} 
			
			boolean bReturn =  ftpClient.storeFile(new String(fileName.getBytes("GBK"), "iso-8859-1"), is);
			ftpClient.changeWorkingDirectory(dir);
			
			return bReturn;
			
		} else  {
			return false;
		}
	}

}
