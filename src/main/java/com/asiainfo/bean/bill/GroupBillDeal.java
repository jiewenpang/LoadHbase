package com.asiainfo.bean.bill;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import com.asiainfo.base.HbaseHelper;
import com.asiainfo.bean.Base;
import com.asiainfo.bean.list.BOSSDeal;

public class GroupBillDeal extends Base {
	static final Log LOG = LogFactory.getLog(GroupBillDeal.class);
	
	public static final String START = "START|"; // 账单起始位置
	public static final String END = "END"; // 账单结束位置

	private boolean isGroupBill = false; // 验证是否为集团账单

	private int BILL_TYPE = 0; // 1=集团新总账单 2=集团明细账单 3=集团代付账单

	public static final char DELIMITER = '|';
	public static final char BODY_ITEM_DELIMITER = '^';

	private String _area; // 地市公司

	/**
	 * 集团新总账单私有
	 */
	private String new_GroupId; // 集团编码
	private String new_BillMonth; // 计费月
	/**
	 * 集团明细账单私有
	 */
	private String detail_GroupId; // 产品编码
	private String detail_ProductId; // 集团产品编码
	private String detail_BillWeek; // 计费周期

	/**
	 * 集团代付款账单数据格式 ,格式与现网数据为主
	 */
	private String df_GroupId; // 集团编号
	private String df_Product; // 产品编号
	private String df_BillMonth; // 计费月

	@Override
	public boolean checkFileName(String name) {
		// Check Hader File JTYJZD_、JTEJZD_、JTDFZD_
		boolean flag = false;
		if (name.toUpperCase().startsWith("JTYJZD_") || name.toUpperCase().startsWith("JTEJZD_")
				|| name.toUpperCase().startsWith("JTDFZD_")) {
			flag = true;
		}
		return flag;
	}

	@Override
	public int buildRecord(HTable table, String filename, BufferedReader br) throws Exception {
		int billcount = 0; // 统计集团账单的条数
		String line = "";
		boolean bflag = false; // 标识账单此条账单是否有错误，如果有错，则整条账单不在组装，直接组装下一条
		StringBuilder body = new StringBuilder();
		String head = "";

		getFileType(filename); // 验证文件类型,获取该文件的地市信息 JTEJZD_NEWGZ201606
		String tmptname = null; // 存储表日期
		// 根据文件的日期建hbase表 JTEJZD_NEWGZ201606 返回 201606
		String tanameame = filename.split("_")[1].substring(5, 11); // 获取集团总账单文件日期
		String taname = new String(table.getTableName()); // 得到已经存在的集团总账单表的日期
		if (taname.endsWith(tanameame)) {
			//表已经存在更新当前表名
			tmptname = taname.substring(0, taname.length()-6) + tanameame; //GROUPBILL_201606
		} else {
			LOG.info("current file start with=" + taname.substring(0, taname.length()-6) + "end with = " + tanameame);
			if(mapTable.containsKey(tanameame) == false){
				if (getRegions().length > 1 || !"".equals(getRegions()[0])) {
					byte[][] regs = new byte[getRegions().length][];
					for (int j = 0; j < getRegions().length; j++) {
						regs[j] = Bytes.toBytes(getRegions()[j]);
					}
					HbaseHelper.creatTable(taname.substring(0, taname.length()-6) + tanameame, getFamilyNames(), regs);
				} else {
					HbaseHelper.creatTable(taname.substring(0, taname.length()-6) + tanameame, getFamilyNames(), null);
				}
				// 更新Htable信息
				table = new HTable(HbaseHelper.conf, Bytes.toBytes(taname.substring(0, taname.length()-6) + tanameame));
				table.setAutoFlush(false);
				table.flushCommits();
				mapTable.put(tanameame, table);
			}else{
				table = mapTable.get(tanameame);
			}
			tmptname = taname.substring(0, taname.length()-6) + tanameame; // 更新当前表信息
		}
		// 设置表名
		setTabName(tmptname);

		while (((line = br.readLine()) != null)) {
			if (line.startsWith(START)) {
				head = line.substring(START.length(), line.length()); // START|
				parseHeader(head);
				body.setLength(0); // 清空数据
				bflag = false;
				continue;
			}

			// 账单体解析
			if (BILL_TYPE == 1 || BILL_TYPE == 2) {
				if ((!line.matches(".*\\|.*\\|.*") || line.split("\\|", -1).length != 3) && !line.startsWith(END)) {
					LOG.error("current type=" + BILL_TYPE + " error context:" + line);
					bflag = true;
				}
			} else {
				//集团代付账单数据长短不一
				if (!line.matches(".*\\|.*\\|.*") && !line.startsWith(END)) {
					LOG.error("current type=" + BILL_TYPE + " error context:" + line);
					bflag = true;
				}
			}

			if (bflag) {
				LOG.error("continue body");
				continue; // 不再拼接账单体，跳过处理下一用户
			} else {
				String reDelimitedLine = line.replace(DELIMITER, BODY_ITEM_DELIMITER);
				if (!line.startsWith(END)) {
					body.append(reDelimitedLine).append("|");
				}
			}

			// 账单尾部;入库操作
			if (line.equals(END)) {
				billcount++;
				
				// 设置列值
				setValues(new String[] { head, _area, body.toString() });

				// 入库
				HbaseHelper.addData(table, getHBaseRowKey(), getFamilyNames()[0], getColumns(), getValues(), "GBK");

			}
		}
		table.flushCommits();
		br.close();
		return billcount;
	}

	private void parseHeader(String header) {
		// 验证是否为集团账单
		if (isGroupBill == true) {
			List<String> split = split(header, '|');

			if (BILL_TYPE == 1) {
				//集团总账单
				//集团编码|集团客户名称|计费月|打印日期|收件人姓名|用户EMAIL地址1~用户EMAIL地址2~... ... ~用户EMAIL地址n
				this.new_GroupId = promisedGet(split, 0); // 集团编号
				this.new_BillMonth = promisedGet(split, 2); // 计费月
			}
			if (BILL_TYPE == 2) {
				//集团明细账单
				//集团编码|集团客户名称|集团产品编码|集团产品名称|计费周期|账户信息时段|计费时段|打印日期|收件人姓名|用户EMAIL地址1~用户EMAIL地址2~... ... ~用户EMAIL地址n
				this.detail_GroupId = promisedGet(split, 0);  //集团编码
				this.detail_ProductId = promisedGet(split, 2);// 集团产品编码
				this.detail_BillWeek = promisedGet(split, 4);// 计费月

			}
			if (BILL_TYPE == 3) {
				//集团代付账单
				if(get_area().equalsIgnoreCase("SZ")){
					//集团编号|集团名称|邮编|联系人地址(城市名称)|联系人|集团代付产品号码|计费月|打印日期|
					this.df_GroupId=promisedGet(split, 0); //集团编号
					this.df_Product=promisedGet(split, 5); //产品编号
					this.df_BillMonth=promisedGet(split, 6); //计费月
				}else{
					//集团产品编号|集团产品名称|集团编号|集团名称|邮编|联系人地址|联系人|联系人手机|计费月|打印日期|
					this.df_GroupId=promisedGet(split, 2); //集团编号
					this.df_Product=promisedGet(split, 0); //产品编号
					this.df_BillMonth=promisedGet(split, 8); //计费月
				}
			}

		}

	}

	/**
	 * 获取rowkey
	 * 
	 * @return
	 */
	public String getHBaseRowKey() {
		StringBuilder sb = new StringBuilder();
		if (isGroupBill) {
			/*
			 * 集团编码|集团账单类型|计费月|地市
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
			 * 集团编码|产品编码|账单类型|计费周期|地市
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

			// 集团编码|产品编号|类型|计费月|地市
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
		return sb.toString();
	}

	/**
	 * 根据文件名获取文件类型
	 * 
	 * @param fileName
	 */
	public void getFileType(String fileName) {
		if (fileName.startsWith("JT")) {
			this.isGroupBill = true; // 数据文件前缀为 JT 表示为集团账单

			if (fileName.contains("JTYJZD")) {
				BILL_TYPE = 1;
			} else if (fileName.contains("JTEJZD")) {
				BILL_TYPE = 2;

			} else if (fileName.contains("JTDFZD")) {
				BILL_TYPE = 3;
			} else {
				LOG.error("无此账单类型");
				return;
			}

			// 集团总账单文件名 JTEJZD_NEWGZ201606 返回地市 GZ
			this._area = fileName.split("_")[1].substring(3, 5);
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

	private List<String> split(String line, char del) {
		List<String> ret = new ArrayList<String>();
		int start = 0;
		while (start < line.length()) {
			int end = line.indexOf(del, start);
			//如果indexOf取值超出最大长度将会返回-1 ,end等于值得最大长度
			if (end == -1) {
				end = line.length();
			}
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
