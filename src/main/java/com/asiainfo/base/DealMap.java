package com.asiainfo.base;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPConnectionClosedException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;

import com.asiainfo.bean.Base;
import com.asiainfo.tool.FtpTools;
import com.asiainfo.tool.LCompress;
public class DealMap extends Mapper<LongWritable, Text, Text, IntWritable>{
	static final Log LOG = LogFactory.getLog(DealMap.class);
	private static final String CHARTSET = "GBK";
	private static String dealcls;
	private static String tbname;
	private static Base dealCls;
	private static String[] regions;
	private static String[] family;
	private static String[] columns;
	private static String filterregion;
	private static String ischgport;
	private static String isbakinput;
	private static String inputlargepath;
	private static int largesize;
	private static String detailoutputdir;
	private static String bakdir;
	private String fileName="";
	private static SimpleDateFormat sf = new SimpleDateFormat("yyyyMMddHH24mmss");
	private HTable table;
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		LOG.info("init");
		//tbname= context.getConfiguration().get("largesize");
		tbname= context.getConfiguration().get("output");
		dealcls=context.getConfiguration().get("dealCls");
		regions=context.getConfiguration().get("region").split(",");
		family=context.getConfiguration().get("family").split(",");
		columns = context.getConfiguration().get("column").split(",");
		filterregion = context.getConfiguration().get("filterregion");
		ischgport = context.getConfiguration().get("ischgport");
		isbakinput = context.getConfiguration().get("isbakinput");
		inputlargepath= context.getConfiguration().get("inputlarge");
		largesize= Integer.valueOf(context.getConfiguration().get("largesize"));
		detailoutputdir = context.getConfiguration().get("detailoutputdir");
		bakdir = context.getConfiguration().get("bakdir");
		LOG.info("filterregion:" + filterregion + ",ischgport:" + ischgport);
		LOG.info("inputlargepath:" + inputlargepath + ",largesize:" + largesize);
		try {
			dealCls=(Base) Class.forName(dealcls).newInstance();
			
			//设置列族
			dealCls.setFamilyNames(family);
			
			//设置列名
			dealCls.setColumns(columns);
			
			//设置分区依据
			dealCls.setRegions(regions);
			
			//设置分区依据
			dealCls.setFilterRegion(filterregion);
			
			
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("loadclsError!", e);
		}
		System.out.println("set conf secusse!");
		table = new HTable(HbaseHelper.conf, Bytes.toBytes(tbname));
		table.setAutoFlush(false);
		table.flushCommits();
		
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		Iterator<Entry<String,FtpTools>> it=FtpTools.ftpClientList.entrySet().iterator();
		LOG.info("cleanup:" + FtpTools.ftpClientList.size());
		FtpTools ftptools = null;
		while(it.hasNext()){
			try {
				ftptools = it.next().getValue();
//				it.next().getValue().disConnect();
				LOG.info("ftptools:"+ftptools.toString());
				if(ftptools.isConned()){
					ftptools.disConnect();
				}
				ftptools = null;
			} catch (Exception e) {
				ftptools = null;
			}
		}
		FtpTools.ftpClientList.clear();
		//close base class table,history month
		if ( !dealCls.mapTable.isEmpty() )
		{
			Iterator<Entry<String,HTable>> it1 = dealCls.mapTable.entrySet().iterator();
			while(it1.hasNext()){
				Entry<String,HTable> entry = it1.next();
				HTable htab = entry.getValue();
				htab.flushCommits();
				htab.close();
			}
			dealCls.mapTable.clear();
		}
		
		//current month
		this.table.flushCommits();
		this.table.close();
	}
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		LOG.info("contents is:"+value.toString());
		String[] ftpinfo=value.toString().split(":");
		//boolean bIsLarge = true;
		//FtpTools ftp=FtpTools.newInstance(ftpinfo[0], Integer.valueOf(ftpinfo[1]), ftpinfo[2], ftpinfo[3], ftpinfo[4]);
		int port = 21;
		if ( ischgport.equals("0") )
			port = Integer.valueOf(ftpinfo[1]);
		
		FtpTools ftp=FtpTools.newInstance(ftpinfo[0], port, ftpinfo[2], ftpinfo[3], ftpinfo[4]);
/*		if(1 == FtpTools.num){
			FtpTools.num = 2;
			System.out.println("sleep a time!");
			Thread.sleep(10*1000);
		}*/
		int	linenum = 0;//文件总行数,无文件头
		int inputlinenum = 0;//实际入库行数
		try {
			if(ftp.connectServer()){
//				System.out.println("connect success!");
				LOG.info("connect success!"+" port:"+port);
				ByteArrayInputStream bin = null;
				FSDataInputStream inStream = null;
				BufferedReader br = null;
				
				if ( largesize < Integer.valueOf(ftpinfo[6]) )
				{
					FileSystem fs=FileSystem.get(context.getConfiguration());
					Path datapath = new Path(inputlargepath+"/"+ftpinfo[5]);
					FSDataOutputStream out=fs.create(datapath);
					ftp.download(ftpinfo[5], out);
					out.close();
					inStream = fs.open(datapath);
					br = new BufferedReader(new InputStreamReader(inStream,CHARTSET));
				}else{
					byte[] bos = ftp.download2Buf(ftpinfo[5]);
					LOG.info("download2Buf finish!");
					if(bos.length==0){
						LOG.info(ftpinfo[5]+"file is empty");
						return;
					}
					if(ftpinfo[5].toLowerCase().endsWith(".z")){//解压
	//					System.out.println("ftp:deCompress");
						LOG.info("ftp:deCompress");
						bos=LCompress.deCompress(bos);
						LOG.info("ftp:deCompress finish");
					}
					 bin = new ByteArrayInputStream(bos);
					 br = new BufferedReader(new InputStreamReader(bin,CHARTSET));
				}
				
				long starttime=System.currentTimeMillis();
				LOG.info("start DealFile:"+ftpinfo[5]);
				HTable tab=this.table;
//				bin.close();
				//业务处理
				linenum = dealCls.buildRecord(tab,ftpinfo[5],br);
				
				long endtime=System.currentTimeMillis();
				LOG.info("insert Hbase Finish!recodeCount:"+linenum+",Time Consuming:"+(endtime-starttime)+"ms.");
				LOG.info(ftpinfo[5]+":process"+linenum);
				if ( largesize > Integer.valueOf(ftpinfo[6]) )
				{
					FileSystem fs=FileSystem.get(context.getConfiguration());
					fs.delete(new Path(inputlargepath+"/"+ftpinfo[5]), true);
				}
				recursiondelfile(context , ftp, ftpinfo, 3, linenum, inputlinenum);
			}else{
				LOG.info("ftp error!");
			}
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			LOG.error("map SocketException:" + e.getMessage());
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			LOG.error("map UnsupportedEncodingException:" + e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error("map IOException:" + e.getMessage());
			e.printStackTrace();
		} catch (Exception e) {
			LOG.error("connect ftp:"+value.toString()+",error!");
			e.printStackTrace();
			StringBuffer sb = new StringBuffer();  
			StackTraceElement[] stackArray = e.getStackTrace();
			for (int i = 0; i < stackArray.length; i++) {  
	            StackTraceElement element = stackArray[i];  
	            if(element.toString().indexOf("asiainfo")!=-1)
	            	sb.append(element.toString() + "\n");  
	        }  
			throw new InterruptedException("element:" + sb.toString());
		}

	}

	//ftp 接口机备份 处理过的文件
	public void detailbak(Context context, String[] ftpinfo, FtpTools ftp,
			int linenum, int inputlinenum) throws UnknownHostException, IOException {
		
		//文件名:JobId_当前处理map主机名_当前处理主机map进程号_yyyymmddhh24miss
		if("".equals(fileName)){
			String hostname = InetAddress.getLocalHost().getHostName();
			String runtime = ManagementFactory.getRuntimeMXBean().getName();
			Integer processnum = Integer.parseInt(runtime.substring(0, runtime.indexOf("@")));;
			String date = sf.format(new Date());
			fileName = context.getJobID().toString()+"_"+hostname+"_"+ processnum +"_"+date;
		}
		
		//文件内容:主机地址|文件名|文件大小|文件记录数|
		String content = ftpinfo[0]+"|"+ftpinfo[5]+"|"+ftpinfo[6]+"|"+linenum+"/r/n";
		InputStream is = new ByteArrayInputStream(content.getBytes());  
		boolean flag = ftp.writeFile(is, detailoutputdir, fileName);
		is.close();
		LOG.info("write detail file isSuccess:" + flag);
	}
	
	private void recursiondelfile(Context context, FtpTools ftp, String[] ftpinfo, int times, int linenum, int inputlinenum){
		if(times <= 0){
			LOG.error("recursiondelfile fail filename:" + ftpinfo[5]);
		}else{
			try {
				if(ftp.connectServer()){
					if(!detailoutputdir.equals("-1")) {
						detailbak(context, ftpinfo, ftp, linenum, inputlinenum);
					}
					if ( "0".equals(isbakinput) )
					{
						LOG.info("delete file:" + ftpinfo[5] + "," + ftp.delete(ftpinfo[5]));
					}else{
						String tofiledir = getTofilename(ftpinfo[4], bakdir);
						FTPClient ftpclent = ftp.getFtpClient();
						if(ftpclent.changeWorkingDirectory(tofiledir) || ftpclent.makeDirectory(tofiledir)) {
							ftp.rename(ftpinfo[4]+"/"+ftpinfo[5], tofiledir+"/"+ftpinfo[5]);
						} else {
							throw new IOException("mkdir bakdir error,bakdir=" + tofiledir);
						}
						
					}
				}else{
					LOG.warn("ftp login fail!");
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				times = times - 1;
				LOG.warn("recursiondelfile IOException:" + e.getMessage());
				e.printStackTrace();
				recursiondelfile(context , ftp, ftpinfo, times, linenum, inputlinenum);
			}
		}
	}
	
	private String getTofilename(String dir,String bakdir)
	{
		int idx = dir.lastIndexOf("/");
		if ( dir.length() == idx+1 )
			idx = dir.lastIndexOf("/", idx-1);
		
		String tofiledir = dir.substring(0, idx) + "/" + bakdir;
		return tofiledir;
	}
}
