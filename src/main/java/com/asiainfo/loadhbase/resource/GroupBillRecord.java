package com.asiainfo.loadhbase.resource;

import java.io.BufferedReader;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;

public class GroupBillRecord extends Record {
	private enum JTBillType {
	    JTZZ, JTMXZD, JTDFZD;
	}
	
	private static final String START = "START|";
	private static final String END = "END";
	private static final char DELIMITER = '|';
	private static final char BODY_ITEM_DELIMITER = '^';
	private String area;			// 地市公司
	private String groupId;			// 集团编码
	private String productId;		// 产品编码，集团总账无此属性
	private String billMonth;		// 计费月
	private JTBillType jtBillType;
	
	@Override
	public boolean checkFileName(String name) {
		return name.toUpperCase().startsWith("JTYJZD_") || name.toUpperCase().startsWith("JTEJZD_")
				|| name.toUpperCase().startsWith("JTDFZD_");
	}

	@Override
	public int buildRecord(String filename, BufferedReader br, Connection connection) throws Exception {
		int billcount = 0; // 统计集团账单的条数
		String line = "";
		boolean bflag = false; // 标识账单此条账单是否有错误，如果有错，则整条账单不在组装，直接组装下一条
		StringBuilder body = new StringBuilder();
		String head = "";
		Table table = null;

		getFileType(filename);

		String tableName = tableNamePrefix + filename.split("_")[1].substring(5, 11);
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
			if (line.startsWith(START)) {
				head = line.substring(START.length(), line.length());
				parseHeader(head);
				body.setLength(0); // 清空数据
				bflag = false;
				continue;
			}

			// 账单体解析
			if (jtBillType == JTBillType.JTZZ || jtBillType == JTBillType.JTMXZD) {
				if ((!line.matches(".*\\|.*\\|.*") || line.split("\\|", -1).length != 3) && !line.startsWith(END)) {
					logger.info("current type=" + jtBillType + " error context:" + line);
					bflag = true;
				}
			} else {
				// 集团代付账单 长度不一
				if (!line.matches(".*\\|.*\\|.*") && !line.startsWith(END)) {
					bflag = true;
				}
			}

			if (bflag) {
				continue; // 不再拼接账单体
			} else {
				String reDelimitedLine = line.replace(DELIMITER, BODY_ITEM_DELIMITER);
				if (!line.startsWith(END)) {
					body.append(reDelimitedLine).append("|");
				}
			}

			// 账单尾部;入库操作
			if (line.equals(END)) {
				billcount++;

				// 入库
				addColumn(table, getRowKey(), getFamilys()[0], getColumns(), new String[] { head, area, body.toString() }, "GBK");

			}
		}
		((HTable) table).flushCommits();
		br.close();
		return billcount;
	}

	public void getFileType(String fileName) {
		if (fileName.contains("JTYJZD")) {
			jtBillType = JTBillType.JTZZ;
		} else if (fileName.contains("JTEJZD")) {
			jtBillType = JTBillType.JTMXZD;
		} else if (fileName.contains("JTDFZD")) {
			jtBillType = JTBillType.JTDFZD;
		} else {
			logger.info("无此账单类型");
			return;
		}

		// 集团总账单文件名 JTEJZD_NEWGZ201606 返回地市 GZ
		area = fileName.split("_")[1].substring(3, 5);
	}

	private void parseHeader(String header) {
		String[] splitHeader = header.split("\\|");

		switch (jtBillType) {
			case JTZZ:
				// 集团编码|集团客户名称|计费月|打印日期|收件人姓名|用户EMAIL地址1~用户EMAIL地址2~... ...
				// ~用户EMAIL地址n
				groupId = GetByIndex(splitHeader, 0);
				billMonth = GetByIndex(splitHeader, 2);
				break;
			case JTMXZD:
				// 集团编码|集团客户名称|集团产品编码|集团产品名称|计费周期|账户信息时段|计费时段|打印日期|收件人姓名|用户EMAIL地址1~用户EMAIL地址2~...
				// ... ~用户EMAIL地址n
				groupId = GetByIndex(splitHeader, 0);
				productId = GetByIndex(splitHeader, 2);
				billMonth = GetByIndex(splitHeader, 4);
				break;
			case JTDFZD:
				if (area.equalsIgnoreCase("SZ")) {
					// 集团编号|集团名称|邮编|联系人地址(城市名称)|联系人|集团代付产品号码|计费月|打印日期|
					groupId = GetByIndex(splitHeader, 0);
					productId = GetByIndex(splitHeader, 5);
					billMonth = GetByIndex(splitHeader, 6);
				} else {
					// 集团产品编号|集团产品名称|集团编号|集团名称|邮编|联系人地址|联系人|联系人手机|计费月|打印日期|
					groupId = GetByIndex(splitHeader, 2);
					productId = GetByIndex(splitHeader, 0);
					billMonth = GetByIndex(splitHeader, 8);
				}
				break;
		}
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
		StringBuilder sb = new StringBuilder();

		switch (jtBillType) {
			case JTZZ:
				// 集团编码|集团账单类型|计费月|地市
				sb.append(groupId).append('|').append(jtBillType).append('|').append(billMonth).append('|').append(area);
				break;
			case JTMXZD:
				// 集团编码|产品编码|集团账单类型|计费月|地市
				sb.append(groupId).append('|').append(productId).append('|').append(jtBillType).append('|').append(billMonth)
						.append('|').append(area);
				break;
			case JTDFZD:
				// 集团编码|产品编码|集团账单类型|计费月|地市
				sb.append(groupId).append('|').append(productId).append('|').append(jtBillType).append('|').append(billMonth)
						.append('|').append(area);
				break;
		}

		return sb.toString();
	}

}
