package com.asiainfo.base;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import com.asiainfo.bean.Base;
import com.asiainfo.bean.FileInfo;
import com.asiainfo.tool.FtpTools;
import com.asiainfo.tool.LCompress;

public class DealClass {
	static final Log LOG = LogFactory.getLog(DealClass.class);
	private static String dealcls;
	private static String tbname;
	private static Base dealCls;
	private static String[] regions;
	private static String[] family;
	private static String[] columns;
	private static String filterregion;
	private static String ischgport;
	private HTable table;
	
	
	public void setup(String tbname,String dealcls,String regions,String family,String columns,String filterregion,String ischgport)
			throws IOException, InterruptedException {
		System.out.println("init DealClass");
		DealClass.tbname= tbname;
		DealClass.dealcls=dealcls;
		DealClass.regions=regions.split(",");
		DealClass.family=family.split(",");
		DealClass.columns = columns.split(",");
		DealClass.filterregion = filterregion;
		DealClass.ischgport = ischgport;
		System.out.println("tbname:" + DealClass.tbname + " dealcls:" + DealClass.dealcls );
		System.out.println("regions:" + regions + " family:" + family + " columns:" + columns);
		System.out.println("filterregion:" + DealClass.filterregion + " ischgport:" + DealClass.ischgport);
		try {
			dealCls=(Base) Class.forName(DealClass.dealcls).newInstance();
			
			//设置列族
			dealCls.setFamilyNames(DealClass.family);
			
			//设置列名
			dealCls.setColumns(DealClass.columns);
			
			//设置分区依据
			dealCls.setRegions(DealClass.regions);
			
			//设置分区依据
			dealCls.setFilterRegion(DealClass.filterregion);
			
			
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("loadclsError!", e);
		}
		System.out.println("设置正常!");
		table = new HTable(HbaseHelper.conf, Bytes.toBytes(DealClass.tbname));
		table.setAutoFlush(false);
		table.flushCommits();
		
	}
	
	
	public void cleanup()
			throws IOException, InterruptedException {
		Iterator<Entry<String,FtpTools>> it=FtpTools.ftpClientList.entrySet().iterator();
		while(it.hasNext()){
			try {
				it.next().getValue().disConnect();
			} catch (Exception e) {
			}
		}
		this.table.flushCommits();
		this.table.close();
	}
	
	public void map(List<FileInfo> fileinfolist)
			throws IOException, InterruptedException {
		for(int i=0;i<fileinfolist.size();i++){
			String value = fileinfolist.get(i).getFileinfo();
			String[] ftpinfo=value.toString().split(":");
			
			//FtpTools ftp=FtpTools.newInstance(ftpinfo[0], Integer.valueOf(ftpinfo[1]), ftpinfo[2], ftpinfo[3], ftpinfo[4]);
			int port = 21;
			if ( ischgport.equals("0") )
				port = Integer.valueOf(ftpinfo[1]);
			
			FtpTools ftp=FtpTools.newInstance(ftpinfo[0], port, ftpinfo[2], ftpinfo[3], ftpinfo[4]);
			System.out.println("contents is"+value.toString()+" port:"+port);
			try {
				if(ftp.connectServer()){
					byte[] bos = ftp.download2Buf(ftpinfo[5]);
					if(bos.length==0){
						LOG.info(ftpinfo[5]+"file is empty");
						return;
					}
					if(ftpinfo[5].toLowerCase().endsWith(".z")){//解压
						bos=LCompress.deCompress(bos);
					}
					ByteArrayInputStream bin=new ByteArrayInputStream(bos);
					BufferedReader br=new BufferedReader(new InputStreamReader(bin));
					long starttime=System.currentTimeMillis();
					LOG.info("start DealFile:"+ftpinfo[5]);
					HTable tab=this.table;
					bin.close();
					//业务处理
					int	linenum = dealCls.buildRecord(tab,ftpinfo[5],br);
					
					long endtime=System.currentTimeMillis();
					LOG.info("insert Hbase Finish!recodeCount:"+linenum+",Time Consuming:"+(endtime-starttime)+"ms.");
					System.out.println(ftpinfo[5]+":process:"+linenum);
					boolean bIsdelete = ftp.delete(ftpinfo[5]);
					System.out.println("delete "+ftpinfo[5]+" :" + bIsdelete);
				}
			} catch (Exception e) {
				System.out.println("connect ftp"+value.toString()+"error!");
				e.printStackTrace();
				StringBuffer sb = new StringBuffer();  
				StackTraceElement[] stackArray = e.getStackTrace();
				for (int ii = 0; ii < stackArray.length; ii++) {  
		            StackTraceElement element = stackArray[ii];  
		            if(element.toString().indexOf("asiainfo")!=-1)
		            	sb.append(element.toString() + "\n");  
		        }  
				throw new InterruptedException(sb.toString());
			}
		}
	}
	
	
	
}
