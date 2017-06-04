package com.asiainfo.loadhbase.resource;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class GroupBillRecord extends Record {
	private static final String START = "START|"; // �˵���ʼλ��
	private static final String END = "END"; // �˵�����λ��
	private static final char DELIMITER = '|';
	private static final char BODY_ITEM_DELIMITER = '^';
	private boolean isGroupBill = false; // ��֤�Ƿ�Ϊ�����˵�
	private int BILL_TYPE = 0; // 1=���������˵� 2=������ϸ�˵� 3=���Ŵ����˵�
	private String _area; // ���й�˾

	/**
	 * ���������˵�˽��
	 */
	private String new_GroupId; // ���ű���
	private String new_Name; // �ͻ�����
	private String new_BillMonth; // �Ʒ���

	/**
	 * ������ϸ�˵�˽��
	 */
	private String detail_GroupId; // ��Ʒ����
	private String detail_ProductId; // ���Ų�Ʒ����
	private String detail_BillWeek; // �Ʒ�����

	/**
	 * ���Ŵ������˵����ݸ�ʽ ,��ʽ����������Ϊ��
	 */
	private String df_GroupId; // ���ű��
	private String df_Product; // ��Ʒ���
	private String df_BillMonth; // �Ʒ���

	@Override
	public boolean checkFileName(String name) {
		// Check Hader File JTYJZD_��JTEJZD_��JTDFZD_
		boolean flag = false;
		if (name.toUpperCase().startsWith("JTYJZD_") || name.toUpperCase().startsWith("JTEJZD_")
				|| name.toUpperCase().startsWith("JTDFZD_")) {
			flag = true;
		}
		return flag;
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

		String tableName = tablePrefix + filename.split("_")[1].substring(5, 11);
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
			if (line.startsWith(START)) {
				head = line.substring(START.length(), line.length()); // START|
				parseHeader(head);
				body.setLength(0); // �������
				bflag = false;
				continue;
			}

			// System.out.println("billcount="+billcount);

			// �˵������
			if (BILL_TYPE == 1 || BILL_TYPE == 2) {
				if ((!line.matches(".*\\|.*\\|.*") || line.split("\\|", -1).length != 3) && !line.startsWith(END)) {
					System.out.println("current type=" + BILL_TYPE + " error context:" + line);
					bflag = true;
				}
			} else {
				// ���Ŵ����˵� ���Ȳ�һ
				if (!line.matches(".*\\|.*\\|.*") && !line.startsWith(END)) {
					// System.out.println("current type="+BILL_TYPE+" error context:"
					// + line);
					bflag = true;
				}
			}

			if (bflag) {
				// System.out.println("continue body");
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
				addColumn(table, getHBaseRowKey(), getFamilys()[0], getColumns(),
						new String[] { head, _area, body.toString() }, "GBK");

			}
		}
		((HTable) table).flushCommits();
		br.close();
		return billcount;
	}

	private void parseHeader(String header) {
		// ��֤�Ƿ�Ϊ�����˵�
		if (isGroupBill == true) {
			List<String> split = split(header, '|');

			if (BILL_TYPE == 1) {
				// �������˵�
				// ���ű���|���ſͻ�����|�Ʒ���|��ӡ����|�ռ�������|�û�EMAIL��ַ1~�û�EMAIL��ַ2~... ...
				// ~�û�EMAIL��ַn
				this.new_GroupId = promisedGet(split, 0); // ���ű���
				this.new_Name = promisedGet(split, 1); // ���ſͻ�����
				this.new_BillMonth = promisedGet(split, 2); // �Ʒ���
			}
			if (BILL_TYPE == 2) {
				// ������ϸ�˵�
				// ���ű���|���ſͻ�����|���Ų�Ʒ����|���Ų�Ʒ����|�Ʒ�����|�˻���Ϣʱ��|�Ʒ�ʱ��|��ӡ����|�ռ�������|�û�EMAIL��ַ1~�û�EMAIL��ַ2~...
				// ... ~�û�EMAIL��ַn
				this.detail_GroupId = promisedGet(split, 0);// ��Ʒ����
				this.detail_ProductId = promisedGet(split, 2);// ���Ų�Ʒ����
				this.detail_BillWeek = promisedGet(split, 4);// �Ʒ�����
			}
			if (BILL_TYPE == 3) {
				// ���Ŵ����˵�
				if (get_area().equalsIgnoreCase("SZ")) {
					// ���ű��|��������|�ʱ�|��ϵ�˵�ַ(��������)|��ϵ��|���Ŵ�����Ʒ����|�Ʒ���|��ӡ����|
					this.df_GroupId = promisedGet(split, 0); // ���ű��
					this.df_Product = promisedGet(split, 5); // ��Ʒ���
					this.df_BillMonth = promisedGet(split, 6); // �Ʒ���
				} else {
					// ���Ų�Ʒ���|���Ų�Ʒ����|���ű��|��������|�ʱ�|��ϵ�˵�ַ|��ϵ��|��ϵ���ֻ�|�Ʒ���|��ӡ����|
					this.df_GroupId = promisedGet(split, 2); // ���ű��
					this.df_Product = promisedGet(split, 0); // ��Ʒ���
					this.df_BillMonth = promisedGet(split, 8); // �Ʒ���
				}

			}

		}

	}

	/**
	 * ��ȡrowkey
	 * 
	 * @return
	 */
	public String getHBaseRowKey() {
		StringBuilder sb = new StringBuilder();
		if (isGroupBill) {
			/*
			 * ���ű���|�����˵�����|�Ʒ���|����
			 */
			if (BILL_TYPE == 1) {
				sb.append(getNew_GroupId());
				sb.append('|');
				sb.append(BILL_TYPE);
				sb.append('|');
				sb.append(getNew_BillMonth());
				sb.append('|');
				sb.append(get_area());
			}

			/**
			 * ���ű���|��Ʒ����|�����˵�����|�Ʒ�����|����
			 */
			if (BILL_TYPE == 2) {
				sb.append(getDetail_GroupId());
				sb.append('|');
				sb.append(getDetail_ProductId());
				sb.append('|');
				sb.append(BILL_TYPE);
				sb.append('|');
				sb.append(getDetail_BillWeek());
				sb.append('|');
				sb.append(get_area());
			}

			// ���ű���|���Ų�Ʒ���|�����˵�����|�Ʒ���|����
			if (BILL_TYPE == 3) {
				sb.append(getDf_GroupId());
				sb.append('|');
				sb.append(getDf_Product());
				sb.append('|');
				sb.append(BILL_TYPE);
				sb.append('|');
				sb.append(getDf_BillMonth());
				sb.append('|');
				sb.append(get_area());
			}
			// code....
		}
		// System.out.println("TYPE="+BILL_TYPE+"  "+sb.toString());
		return sb.toString();
	}

	/**
	 * �����ļ�����ȡ�ļ�����
	 * 
	 * @param fileName
	 */
	public void getFileType(String fileName) {
		if (fileName.startsWith("JT")) {
			this.isGroupBill = true; // �����ļ�ǰ׺Ϊ JT ��ʾΪ�����˵�

			if (fileName.contains("JTYJZD")) {
				BILL_TYPE = 1;
			} else if (fileName.contains("JTEJZD")) {
				BILL_TYPE = 2;

			} else if (fileName.contains("JTDFZD")) {
				BILL_TYPE = 3;
			} else {
				System.out.println("�޴��˵�����");
				return;
			}

			// �������˵��ļ��� JTEJZD_NEWGZ201606 ���ص��� GZ
			this._area = fileName.split("_")[1].substring(3, 5);
		}
	}

	/**
	 * ��ȡ������Ԫ��
	 * 
	 * @param split
	 *            Դ����
	 * @param index
	 *            ����λ��
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

	private List<String> split(String line, char del) {
		List<String> ret = new ArrayList<String>();
		int start = 0;
		while (start < line.length()) {
			int end = line.indexOf(del, start);
			if (end == -1) {
				end = line.length();
			}
			// System.out.println("split = " + line.substring(start, end));
			ret.add(line.substring(start, end));
			start = end + 1;
		}
		return ret;
	}

	public String get_area() {
		return _area;
	}

	public void set_area(String _area) {
		this._area = _area;
	}

	public String getNew_GroupId() {
		return new_GroupId;
	}

	public void setNew_GroupId(String new_GroupId) {
		this.new_GroupId = new_GroupId;
	}

	public String getNew_Name() {
		return new_Name;
	}

	public void setNew_Name(String new_Name) {
		this.new_Name = new_Name;
	}

	public String getNew_BillMonth() {
		return new_BillMonth;
	}

	public void setNew_BillMonth(String new_BillMonth) {
		this.new_BillMonth = new_BillMonth;
	}

	public String getDetail_GroupId() {
		return detail_GroupId;
	}

	public void setDetail_GroupId(String detail_GroupId) {
		this.detail_GroupId = detail_GroupId;
	}

	public String getDetail_ProductId() {
		return detail_ProductId;
	}

	public void setDetail_ProductId(String detail_ProductId) {
		this.detail_ProductId = detail_ProductId;
	}

	public String getDetail_BillWeek() {
		return detail_BillWeek;
	}

	public void setDetail_BillWeek(String detail_BillWeek) {
		this.detail_BillWeek = detail_BillWeek;
	}

	public String getDf_GroupId() {
		return df_GroupId;
	}

	public void setDf_GroupId(String df_GroupId) {
		this.df_GroupId = df_GroupId;
	}

	public String getDf_Product() {
		return df_Product;
	}

	public void setDf_Product(String df_Product) {
		this.df_Product = df_Product;
	}

	public String getDf_BillMonth() {
		return df_BillMonth;
	}

	public void setDf_BillMonth(String df_BillMonth) {
		this.df_BillMonth = df_BillMonth;
	}

}
