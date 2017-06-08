package com.asiainfo.loadhbase.resource;

import java.io.BufferedReader;
import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;

public class BillRecord extends Record {
	private static final String START = "START|";
	private static final String END = "END";
	private static final String DELIMITER = "|";
	private static final String BODY_ITEM_DELIMITER = "^";

	private boolean isBillVerionOne = false;
	private String area;
	private String mobNo;
	private String acctNo;
	private String yearMonth;
	private String version;
	private String type;

	@Override
	public boolean checkFileName(String name) {
		return name.toUpperCase().startsWith("CXBILL_") || name.toUpperCase().startsWith("HWBILL_")
				|| name.toUpperCase().startsWith("CXBILLNEW_") || name.toUpperCase().startsWith("HWBILLNEW_")
				|| name.toUpperCase().startsWith("CXFLOWBILL_") || name.toUpperCase().startsWith("HWFLOWBILL_");
	}

	@Override
	public int buildRecord(String filename, BufferedReader br, Connection connection) throws Exception {
		int billCount = 0;
		String line = "";
		boolean bflag = false; // 标识账单此条账单是否有错误，如果有错，则整条账单不在组装，直接组装下一条
		StringBuilder body = new StringBuilder();
		String head = "";
		Table table = null;

		getFileType(filename);
		// 按文件名的日期建表

		String tableName = tableNamePrefix + filename.split("_")[2].substring(0, 6);
		logger.info("currTableName:" + tableName);

		table = mapTable.get(tableName);
		if (table == null) {
			creatTable(tableName, getFamilys(), regions, connection);
			table = connection.getTable(TableName.valueOf(tableName));
			((HTable) table).setAutoFlushTo(false);
			((HTable) table).flushCommits();
			mapTable.put(tableName, table);
		}

		while (((line = br.readLine()) != null)) {

			// 账单头解析
			if (line.startsWith(START)) {
				head = line.substring(START.length(), line.length());
				// 解析账单头
				parseHeader(head);
				body.setLength(0); // 清空之前body数据 效率稍微好点
				bflag = false;
				continue;
			}

			// 账单体解析
			if ((!line.matches(".*\\|.*\\|.*") || line.split("\\|", -1).length != 3) && !line.startsWith(END)) {
				logger.info("error mobile:" + mobNo + "error context:" + line);
				bflag = true;
			}

			if (bflag) {
				continue;
			} else {
				String reDelimitedLine = line.replace(DELIMITER, BODY_ITEM_DELIMITER);
				if (!line.startsWith(END)) {
					body.append(reDelimitedLine).append("|");
				}
			}

			// 账单尾部和入库
			if (line.equals(END)) {
				billCount++;
				addColumn(table, getRowKey(), getFamilys()[0], getColumns(), new String[] { head, area, body.toString() }, "GBK");

			}
		}
		((HTable) table).flushCommits();
		br.close();
		return billCount;
	}

	public void getFileType(String fileName) throws IOException {
		isBillVerionOne = (fileName.contains("NEW") || fileName.contains("FLOW")) ? false : true;
		area = fileName.split("_")[1];
	}

	private void parseHeader(String header) {
		String[] splitHeader = header.split("\\|");

		acctNo = GetByIndex(splitHeader, 0);
		mobNo = GetByIndex(splitHeader, 2);
		yearMonth = GetByIndex(splitHeader, 4);
		version = isBillVerionOne ? "" : GetByIndex(splitHeader, -5);
		type = isBillVerionOne ? "" : GetByIndex(splitHeader, -4);
	}

	private String GetByIndex(String[] splitHeader, int index) {
		if (index >= splitHeader.length) {
			return "";
		} else if (index < 0) {
			index += splitHeader.length;
		}
		
		return splitHeader[index];
	}
	
	public String getRowKey() {
		StringBuilder rowKey = new StringBuilder();

		rowKey.append(mobNo).append('|').append(acctNo).append('|').append(yearMonth).append('|').append(area).append('|');
		if (!isBillVerionOne) {
			rowKey.append(version).append('|').append(type);
		}
		return rowKey.toString();
	}
}
