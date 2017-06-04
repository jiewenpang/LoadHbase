package com.asiainfo.loadhbase.handler;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import com.asiainfo.loadhbase.tool.FtpTools;
import com.asiainfo.loadhbase.tool.LCompress;

public class NoMRHander extends BaseHandler {

	@Override
	public void run() throws Exception { // 单进程处理模式

		// 获取输入文件信息列表
		List<FileInfo> fileInfoList = new ArrayList<FileInfo>();
		GetEveryFileInfo(fileInfoList);
		int fileNum = fileInfoList.size();
		if (fileNum == 0) {
			logger.info("task finish,quit!");
			return;
		}
		logger.info("Get fileInfoList success, fileNum:" + fileNum);

		// 执行任务
		long beginTime = System.currentTimeMillis();
		ProcessFile(fileInfoList);
		cleanup();
		long totalTime = (System.currentTimeMillis() - beginTime) / 1000;
		logger.info("Excute NoMRHander success! total time(s):" + totalTime);
	}

	public void ProcessFile(List<FileInfo> fileinfolist) throws IOException, InterruptedException {
		for (int i = 0; i < fileinfolist.size(); i++) {
			String value = fileinfolist.get(i).getFileinfo();
			String[] ftpinfo = value.toString().split(":");

			int port = 21;
			if (!isUseDefaultPort)
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
					logger.info("insert Hbase Finish!recodeCount:" + linenum + ",Time Consuming:" + (endtime - starttime) + "ms.");
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
	
	public void cleanup() throws IOException, InterruptedException {
		Iterator<Entry<String, FtpTools>> it = FtpTools.ftpClientList.entrySet().iterator();
		while (it.hasNext()) {
			try {
				it.next().getValue().disConnect();
			} catch (Exception e) {
			}
		}
	}

}
