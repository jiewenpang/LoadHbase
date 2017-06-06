package com.asiainfo.loadhbase.handler;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class NoMRHander extends BaseHandler {

	@Override
	public void run() throws Exception { // �����̴���ģʽ

		// ��ȡ�����ļ���Ϣ�б�
		List<FileInfo> fileInfoList = new ArrayList<FileInfo>();
		GetEveryFileInfo(fileInfoList);
		int fileNum = fileInfoList.size();
		if (fileNum == 0) {
			logger.info("task finish,quit!");
			return;
		}
		logger.info("Get fileInfoList success, fileNum:" + fileNum);

		// ִ������
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
			
			MapProcessOneFile(hbaseConfiguration, record, ftpinfo, maxFileHandlePath, Long.parseLong(maxFileSize), 
					detailOutputPath, inputBakPath, detailOutputFileName, 
					"PutHbaseJOB_NoMRHander"+ new SimpleDateFormat("yyyyMM").format(new Date()));
		}
	}
	
	public void cleanup() throws IOException, InterruptedException {
		MapCleanUp(record, maxFileHandlePath, detailOutputPath, detailOutputFileName);
	}

}
