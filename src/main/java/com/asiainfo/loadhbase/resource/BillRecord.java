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
	public static final String START = "START|";
	public static final String END = "END";

	public static int ACCNO_INDEX = 0;
	public static int USERID_INDEX = 1;
	public static int MOBNO_INDEX = 2;
	public static int DATE_INDEX = 4;
	public static int VERSION_INDEX = -5;
	public static int TYPE_INDEX = -4;
	public static int OLD_BILL_LENGTH = 20;

	public static final char DELIMITER = '|';
	public static final char BODY_ITEM_DELIMITER = '^';

	private boolean _isGroupBill = false;
	private boolean _isOldBill = false;

	private String _mobNo;
	private String _accNo;
	private String _userId;
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
				creatTable(tableName, getFamilyNames(), regs, connection);
			} else {
				creatTable(tableName, getFamilyNames(), null, connection);
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
				System.out.println("error mobile:" + get_mobNo() + "error context:" + line);
				bflag = true;
			}

			if (bflag) {
				continue; // 不再拼接账单体
			} else {
				String reDelimitedLine = line.replace(DELIMITER, BODY_ITEM_DELIMITER);
				if (!line.startsWith(END))
					body.append(reDelimitedLine).append("|");
			}

			// 账单尾部;入库操作
			if (line.equals(END)) {
				billcount++;

				/*
				 * //设置列名 if(getColumns() == null || getColumns().length<=0) {
				 * setColumns(new String[]{"Header","Area","Body"}); }
				 */

				// 入库
				addColumn(table, getHBaseRowKey(), getFamilyNames()[0], getColumns(),
						new String[] { head, _area, body.toString() }, "GBK");

			}
		}
		((HTable) table).flushCommits();
		br.close();
		return billcount;
	}

	/**
	 * 获取rowkey
	 * 
	 * @return
	 */
	public String getHBaseRowKey() {
		StringBuilder sb = new StringBuilder();
		if (this._isGroupBill) {
			sb.append(get_accNo());
			sb.append('|');
			sb.append(get_mobNo());
		} else {
			sb.append(get_mobNo());
			sb.append('|');
			sb.append(get_accNo());
			sb.append('|');
			sb.append(get_yearMonth());
			sb.append('|');
			sb.append(get_area());
			sb.append('|');
			if (this._isOldBill) {
				sb.append('|');
			} else {
				sb.append(get_version());
				sb.append('|');
				sb.append(get_type());
			}
		}
		return sb.toString();
	}

	/**
	 * 从账单头获取相关讯息
	 * 
	 * @param header
	 */
	private void parseHeader(String header) {
		List<String> split = split(header, '|');

		this._accNo = promisedGet(split, ACCNO_INDEX);
		this._userId = promisedGet(split, USERID_INDEX);
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

	/**
	 * 获取集合中元素
	 * 
	 * @param split
	 *            源数据
	 * @param index
	 *            索引位置
	 * @return
	 */
	private String promisedGet(List<String> split, int index) {
		if (index >= split.size()) {
			return "";
		}
		if (index < 0) {
			return (String) split.get(split.size() + index);
		}
		return (String) split.get(index);
	}

	/**
	 * 根据文件名获取文件类型
	 * 
	 * @param fileName
	 * @throws IOException
	 */
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

	public String get_mobNo() {
		return _mobNo;
	}

	public void set_mobNo(String _mobNo) {
		this._mobNo = _mobNo;
	}

	public String get_accNo() {
		return _accNo;
	}

	public void set_accNo(String _accNo) {
		this._accNo = _accNo;
	}

	public String get_userId() {
		return _userId;
	}

	public void set_userId(String _userId) {
		this._userId = _userId;
	}

	public String get_yearMonth() {
		return _yearMonth;
	}

	public void set_yearMonth(String _yearMonth) {
		this._yearMonth = _yearMonth;
	}

	public String get_version() {
		return _version;
	}

	public void set_version(String _version) {
		this._version = _version;
	}

	public String get_type() {
		return _type;
	}

	public void set_type(String _type) {
		this._type = _type;
	}

	public String get_area() {
		return _area;
	}

	public void set_area(String _area) {
		this._area = _area;
	}

}
