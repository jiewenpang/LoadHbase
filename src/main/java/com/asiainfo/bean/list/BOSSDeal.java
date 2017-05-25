package com.asiainfo.bean.list;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import com.asiainfo.base.DealClass;
import com.asiainfo.base.HbaseHelper;
import com.asiainfo.bean.Base;

public class BOSSDeal extends Base {
	static final Log LOG = LogFactory.getLog(BOSSDeal.class);
	
	@Override
	public boolean checkFileName(String name) {
		boolean flag=false;
		if(name.toLowerCase().startsWith("boss.")){
			flag=true;
		}
		return flag;
	}
	/**
	 * 处理入库
	 */
	@Override
	public int buildRecord(HTable table,
						   String filename,
						   BufferedReader br)  throws InterruptedException{
		int linenum = 0;
		String line = "";
//		StringBuilder sb = new StringBuilder();
		StringBuilder sb = null;
//		System.out.println("begin  buildRecord!");
		LOG.info("begin  buildRecord!");
		HTable tab = table;
			try {
				while( (line=br.readLine()) != null ) {
					sb=new StringBuilder(line);
					linenum++;
					//按照第一行建表
					if(linenum == 1) {
						String tbname=sb.substring(139, 145);
						String taname=new String(table.getTableName());
						if(taname.endsWith(tbname)) {
							//表已经存在，不需要重新创建
							continue;
						}
						
						String tbn=new String(table.getTableName());
//						System.out.println("currFileMonth:" + tbname);
						LOG.info("currFileMonth:" + tbname);
						
						if ( mapTable.containsKey(tbname) == false )
						{
							LOG.info("begin create table: "+tbn.substring(0, tbn.length()-6)+tbname);
							if(getRegions().length>1 || !"".equals(getRegions()[0])){
								byte[][] regs=new byte[getRegions().length][];
								for(int j=0;j<getRegions().length;j++){
									regs[j]=Bytes.toBytes(getRegions()[j]);
								}
								HbaseHelper.creatTable(tbn.substring(0, tbn.length()-6)+tbname,getFamilyNames(),regs);
							}else{
								HbaseHelper.creatTable(tbn.substring(0, tbn.length()-6)+tbname,getFamilyNames(),null);
							}
							
							tab=new HTable(HbaseHelper.conf, Bytes.toBytes(tbn.substring(0, tbn.length()-6)+tbname));
							tab.setAutoFlush(false);
							tab.flushCommits();
							
							mapTable.put(tbname, tab);
						}else{
							tab = mapTable.get(tbname);
						}

						String tmptname = tbn.substring(0, tbn.length()-6) + tbname;
						//设置表名
						setTabName(tmptname);
						continue;
					}
					
					String telnum=null;
					String time=null;
					String area=null;
					String type=filename.substring(5,7);
					if("01".equals(type) || "02".equals(type)|| "03".equals(type)|| "04".equals(type)|| "05".equals(type)){
						area=sb.substring(125, 127);
						telnum=sb.substring(21, 45).trim();
						time=sb.substring(75, 89);
					}else if("06".equals(type) || "07".equals(type)){
						area=sb.substring(57, 59);
						telnum=sb.substring(19, 43).trim();
						if("06".equals(type))time=sb.substring(131,145);
						else if("07".equals(type))time=sb.substring(146,160);
					}else if("11".equals(type) || "12".equals(type)|| "13".equals(type)|| "15".equals(type)){
						area=sb.substring(149, 151);
						telnum=sb.substring(21, 45).trim();
						time=sb.substring(75,89);
					}
					if(linenum == 2 && filterregion.contains(area)){
						LOG.info("filter region:" + area);
						//过滤部分地市
						break;
					}
					
					//设置行键
					StringBuilder hsb = new StringBuilder();
					hsb.append(telnum).append("|").append(time).append("|").append(filename).append("|").append(linenum);

					//设置列名
					//if(getColumns() == null || getColumns().length<=0) {
						//setColumns(new String[]{"EventFormatType","Area","Cdr"});
					//}
					
					//设置列值
					setValues(new String[]{line});
					//入库
					HbaseHelper.addData(tab,
							hsb.toString(),
							getFamilyNames()[0],
							getColumns(),
							getValues(),
							null);
				
				}
				tab.flushCommits();
				//if( tab!=table ) tab.close();
				if( null != br) br.close();
				LOG.info("end buildRecord");
			} catch (MasterNotRunningException e) {
				// TODO Auto-generated catch block
				LOG.error("MasterNotRunningException:" + e.getMessage());
//				System.out.println("MasterNotRunningException:" + e.getMessage());
				throw new InterruptedException();
			} catch (ZooKeeperConnectionException e) {
				// TODO Auto-generated catch block
				LOG.error("ZooKeeperConnectionException:" + e.getMessage());
				throw new InterruptedException();
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				LOG.error("UnsupportedEncodingException:" + e.getMessage());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				LOG.error("IOException:" + e.getMessage());
			}
		
		
		
		return linenum-1;
	}
}
