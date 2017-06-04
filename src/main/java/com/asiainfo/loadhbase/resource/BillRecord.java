package com.asiainfo.loadhbase.resource;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BillRecord extends Record {
	protected static final Logger logger = LoggerFactory.getLogger(BillRecord.class);
	
	private static final String START = "START|";
	private static final String END = "END";
	private static final int ACCNO_INDEX = 0;
	private static final int MOBNO_INDEX = 2;
	private static final int DATE_INDEX = 4;
	private static final int VERSION_INDEX = -5;
	private static final int TYPE_INDEX = -4;
	private static final char DELIMITER = '|';
	private static final char BODY_ITEM_DELIMITER = '^';

	private boolean _isGroupBill = false;
	private boolean _isOldBill = false;
	private String _mobNo;
	private String _accNo;
	private String _yearMonth;
	private String _version;
	private String _type;
	private String _area;

	@Override
	public boolean checkFileName(String name) {
		/**
		 * CXBILL、HWBILL、CXBILLNEW、HWBILLNEW、CXFLOWBILL、HWFLOWBILL
		 */
		boolean flag = false;
		if (name.toUpperCase().startsWith("CXBILL_") || name.toUpperCase().startsWith("HWBILL_")
				|| name.toUpperCase().startsWith("CXBILLNEW_") || name.toUpperCase().startsWith("HWBILLNEW_")
				|| name.toUpperCase().startsWith("CXFLOWBILL_") || name.toUpperCase().startsWith("HWFLOWBILL_")) {
			flag = true;
		}
		return flag;
	}

	@Override
	public int buildRecord(String filename, BufferedReader br, Connection connection) throws Exception {
		int billcount = 0;
		String line = "";
		boolean bflag = false; // 标识账单此条账单是否有错误，如果有错，则整条账单不在组装，直接组装下一条
		StringBuilder body = new StringBuilder();
		String head = "";
		Table table = null;

		getFileType(filename);
		// 按文件名的日期建表

		String tableName = tablePrefix + filename.split("_")[2].substring(0, 6);
		System.out.println("currTableName:" + tableName);

		table = mapTable.get(tableName);
		if (table == null) {
			if (getRegions().length > 1 || !"".equals(getRegions()[0])) {
				byte[][] regs = new byte[getRegions().length][];
				for (int j = 0; j < getRegions().length; j++) {
					regs[j] = Bytes.toBytes(getRegions()[j]);
				}
				creatTable(tableName, getFamilys(), regs, connection);
			} else {
				creatTable(tableName, getFamilys(), null, connection);
			}

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
				// body.delete(0, body.length()); //清空之前body数据
				body.setLength(0); // 清空之前body数据 效率稍微好点
				bflag = false;
				continue;
			}

			// 账单体解析
			if ((!line.matches(".*\\|.*\\|.*") || line.split("\\|", -1).length != 3) && !line.startsWith(END)) {
				System.out.println("error mobile:" + _mobNo + "error context:" + line);
				bflag = true;
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
				addColumn(table, getHBaseRowKey(), getFamilys()[0], getColumns(), new String[] { head, _area, body.toString() }, "GBK");

			}
		}
		((HTable) table).flushCommits();
		br.close();
		return billcount;
	}

	public String getHBaseRowKey() {
		StringBuilder sb = new StringBuilder();
		if (this._isGroupBill) {
			sb.append(_accNo).append('|').append(_mobNo);
		} else {
			sb.append(_mobNo).append('|').append(_accNo).append('|').append(_yearMonth).append('|').append(_area).append('|');
			if (this._isOldBill) {
				sb.append('|');
			} else {
				sb.append(_version);
				sb.append('|');
				sb.append(_type);
			}
		}
		return sb.toString();
	}

	private void parseHeader(String header) {
		List<String> split = split(header, '|');

		this._accNo = promisedGet(split, ACCNO_INDEX);
		this._mobNo = promisedGet(split, MOBNO_INDEX);
		this._yearMonth = promisedGet(split, DATE_INDEX);
		if (this._isOldBill) {
			this._version = "";
			this._type = "";
		} else {
			this._version = promisedGet(split, VERSION_INDEX);
			this._type = promisedGet(split, TYPE_INDEX);
		}
	}

	private String promisedGet(List<String> split, int index) {
		if (index >= split.size()) {
			return "";
		}
		if (index < 0) {
			return (String) split.get(split.size() + index);
		}
		return (String) split.get(index);
	}

	public void getFileType(String fileName) throws IOException {
		if (fileName.startsWith("JT")) {
			this._isGroupBill = true;
			this._area = "";
		} else {
			this._isGroupBill = false;
			if ((fileName.contains("NEW")) || (fileName.contains("FLOW"))) {
				this._isOldBill = false;
			} else {
				this._isOldBill = true;
			}
			this._area = fileName.split("_")[1];
		}
	}

	private List<String> split(String line, char del) {
		List<String> ret = new ArrayList<String>();
		int start = 0;
		while (start < line.length()) {
			int end = line.indexOf(del, start);
			if (end == -1) {
				end = line.length();
			}
			ret.add(line.substring(start, end));
			start = end + 1;
		}
		return ret;
	}

}
