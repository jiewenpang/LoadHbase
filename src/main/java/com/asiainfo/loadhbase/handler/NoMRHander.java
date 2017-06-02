package com.asiainfo.loadhbase.handler;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.HTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asiainfo.loadhbase.resource.Record;
import com.asiainfo.loadhbase.tool.FtpTools;
import com.asiainfo.loadhbase.tool.LCompress;

public class NoMRHander extends BaseHandler {

	@Override
	public void run() throws Exception { // 单进程处理模式

		List<FileInfo> fileInfoList = new ArrayList<FileInfo>();
		Long totalsize = GetEveryFileInfo(fileInfoList);
		int filenum = fileInfoList.size();
		Long avgfilesize = totalsize / maptotalnum; // 每个map处理的文件总大小
		if (filenum == 0) {
			logger.info("task finish,quit!");
			return;
		}
		logger.info("ftp list finish:" + filenum + ",avgfilesize:" + avgfilesize + "");

		long sttime = System.currentTimeMillis();
		NoMRClass Deal = new NoMRClass();
		Deal.setup(recordClassName, region, tabFamily, column, filterregion, ischgport);
		Deal.ProcessFile(fileInfoList);
		Deal.cleanup();
		long endtime = System.currentTimeMillis();
		logger.info("file list:" + fileInfoList.size() + ",all task finish,total time:" + (endtime - sttime) / 1000
				+ "s,quit...");
	}

	private static class NoMRClass {
		protected static final Logger logger = LoggerFactory.getLogger(NoMRClass.class);
		private static String recordClassName;
		private static Record record;
		private static String[] regions;
		private static String[] family;
		private static String[] columns;
		private static String filterregion;
		private static String ischgport;
		private HTable table;

		public void setup(String recordClassName, String regions, String family, String columns, String filterregion,
				String ischgport) throws IOException, InterruptedException {
			NoMRClass.recordClassName = recordClassName;
			NoMRClass.regions = regions.split(",");
			NoMRClass.family = family.split(",");
			NoMRClass.columns = columns.split(",");
			NoMRClass.filterregion = filterregion;
			NoMRClass.ischgport = ischgport;
			try {
				record = (Record) Class.forName(NoMRClass.recordClassName).newInstance();
				record.setFamilyNames(NoMRClass.family);
				record.setColumns(NoMRClass.columns);
				record.setRegions(NoMRClass.regions);
				record.setFilterRegion(NoMRClass.filterregion);
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("loadclsError!", e);
			}
			System.out.println("设置正常!");
		}

		public void cleanup() throws IOException, InterruptedException {
			Iterator<Entry<String, FtpTools>> it = FtpTools.ftpClientList.entrySet().iterator();
			while (it.hasNext()) {
				try {
					it.next().getValue().disConnect();
				} catch (Exception e) {
				}
			}
			this.table.flushCommits();
			this.table.close();
		}

		public void ProcessFile(List<FileInfo> fileinfolist) throws IOException, InterruptedException {
			for (int i = 0; i < fileinfolist.size(); i++) {
				String value = fileinfolist.get(i).getFileinfo();
				String[] ftpinfo = value.toString().split(":");

				int port = 21;
				if (ischgport.equals("0"))
					port = Integer.valueOf(ftpinfo[1]);

				FtpTools ftp = FtpTools.newInstance(ftpinfo[0], port, ftpinfo[2], ftpinfo[3], ftpinfo[4]);
				System.out.println("contents is" + value.toString() + " port:" + port);
				try {
					if (ftp.connectServer()) {
						byte[] bos = ftp.download2Buf(ftpinfo[5]);
						if (bos.length == 0) {
							logger.info(ftpinfo[5] + "file is empty");
							return;
						}
						if (ftpinfo[5].toLowerCase().endsWith(".z")) {// 解压
							bos = LCompress.deCompress(bos);
						}
						ByteArrayInputStream bin = new ByteArrayInputStream(bos);
						BufferedReader br = new BufferedReader(new InputStreamReader(bin));
						long starttime = System.currentTimeMillis();
						logger.info("start DealFile:" + ftpinfo[5]);
						bin.close();
						// 业务处理
						int linenum = record.buildRecord(ftpinfo[5], br, null);

						long endtime = System.currentTimeMillis();
						logger.info("insert Hbase Finish!recodeCount:" + linenum + ",Time Consuming:"
								+ (endtime - starttime) + "ms.");
						System.out.println(ftpinfo[5] + ":process:" + linenum);
						boolean bIsdelete = ftp.delete(ftpinfo[5]);
						System.out.println("delete " + ftpinfo[5] + " :" + bIsdelete);
					}
				} catch (Exception e) {
					System.out.println("connect ftp" + value.toString() + "error!");
					e.printStackTrace();
					StringBuffer sb = new StringBuffer();
					StackTraceElement[] stackArray = e.getStackTrace();
					for (int ii = 0; ii < stackArray.length; ii++) {
						StackTraceElement element = stackArray[ii];
						if (element.toString().indexOf("asiainfo") != -1)
							sb.append(element.toString() + "\n");
					}
					throw new InterruptedException(sb.toString());
				}
			}
		}

	}

}
