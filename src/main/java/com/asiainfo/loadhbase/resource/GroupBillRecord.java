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
	private String area;			// ���й�˾
	private String groupId;			// ���ű���
	private String productId;		// ��Ʒ���룬���������޴�����
	private String billMonth;		// �Ʒ���
	private JTBillType jtBillType;
	
	@Override
	public boolean checkFileName(String name) {
		return name.toUpperCase().startsWith("JTYJZD_") || name.toUpperCase().startsWith("JTEJZD_")
				|| name.toUpperCase().startsWith("JTDFZD_");
	}

	@Override
	public int buildRecord(String filename, BufferedReader br, Connection connection) throws Exception {
		int billcount = 0; // ͳ�Ƽ����˵�������
		String line = "";
		boolean bflag = false; // ��ʶ�˵������˵��Ƿ��д�������д��������˵�������װ��ֱ����װ��һ��
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
				body.setLength(0); // �������
				bflag = false;
				continue;
			}

			// �˵������
			if (jtBillType == JTBillType.JTZZ || jtBillType == JTBillType.JTMXZD) {
				if ((!line.matches(".*\\|.*\\|.*") || line.split("\\|", -1).length != 3) && !line.startsWith(END)) {
					logger.info("current type=" + jtBillType + " error context:" + line);
					bflag = true;
				}
			} else {
				// ���Ŵ����˵� ���Ȳ�һ
				if (!line.matches(".*\\|.*\\|.*") && !line.startsWith(END)) {
					bflag = true;
				}
			}

			if (bflag) {
				continue; // ����ƴ���˵���
			} else {
				String reDelimitedLine = line.replace(DELIMITER, BODY_ITEM_DELIMITER);
				if (!line.startsWith(END)) {
					body.append(reDelimitedLine).append("|");
				}
			}

			// �˵�β��;������
			if (line.equals(END)) {
				billcount++;

				// ���
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
			logger.info("�޴��˵�����");
			return;
		}

		// �������˵��ļ��� JTEJZD_NEWGZ201606 ���ص��� GZ
		area = fileName.split("_")[1].substring(3, 5);
	}

	private void parseHeader(String header) {
		String[] splitHeader = header.split("\\|");

		switch (jtBillType) {
			case JTZZ:
				// ���ű���|���ſͻ�����|�Ʒ���|��ӡ����|�ռ�������|�û�EMAIL��ַ1~�û�EMAIL��ַ2~... ...
				// ~�û�EMAIL��ַn
				groupId = GetByIndex(splitHeader, 0);
				billMonth = GetByIndex(splitHeader, 2);
				break;
			case JTMXZD:
				// ���ű���|���ſͻ�����|���Ų�Ʒ����|���Ų�Ʒ����|�Ʒ�����|�˻���Ϣʱ��|�Ʒ�ʱ��|��ӡ����|�ռ�������|�û�EMAIL��ַ1~�û�EMAIL��ַ2~...
				// ... ~�û�EMAIL��ַn
				groupId = GetByIndex(splitHeader, 0);
				productId = GetByIndex(splitHeader, 2);
				billMonth = GetByIndex(splitHeader, 4);
				break;
			case JTDFZD:
				if (area.equalsIgnoreCase("SZ")) {
					// ���ű��|��������|�ʱ�|��ϵ�˵�ַ(��������)|��ϵ��|���Ŵ�����Ʒ����|�Ʒ���|��ӡ����|
					groupId = GetByIndex(splitHeader, 0);
					productId = GetByIndex(splitHeader, 5);
					billMonth = GetByIndex(splitHeader, 6);
				} else {
					// ���Ų�Ʒ���|���Ų�Ʒ����|���ű��|��������|�ʱ�|��ϵ�˵�ַ|��ϵ��|��ϵ���ֻ�|�Ʒ���|��ӡ����|
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
				// ���ű���|�����˵�����|�Ʒ���|����
				sb.append(groupId).append('|').append(jtBillType).append('|').append(billMonth).append('|').append(area);
				break;
			case JTMXZD:
				// ���ű���|��Ʒ����|�����˵�����|�Ʒ���|����
				sb.append(groupId).append('|').append(productId).append('|').append(jtBillType).append('|').append(billMonth)
						.append('|').append(area);
				break;
			case JTDFZD:
				// ���ű���|��Ʒ����|�����˵�����|�Ʒ���|����
				sb.append(groupId).append('|').append(productId).append('|').append(jtBillType).append('|').append(billMonth)
						.append('|').append(area);
				break;
		}

		return sb.toString();
	}

}
