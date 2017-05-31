package com.asiainfo.loadhbase.resource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HeFeiXinRecord extends Record {
	public static final Log LOG = LogFactory.getLog(HeFeiXinRecord.class);
	public static final String END = "90";
	private static int MAX_SEQ_LENGTH = 9;

	@Override
	public boolean checkFileName(String name) {
		boolean flag = false;
		if (name.startsWith("NMS")) {
			if (name.length() == 18) {
				flag = true;
			}
		}
		return flag;
	}

	@Override
	public int buildRecord(String filename, BufferedReader br, Connection connection) throws InterruptedException {
		int linenum = 0; // 行号
		String line = ""; // 每行数据
		Table table = null;
		String tableName = null;
		
		try {
			while ((line = br.readLine()) != null) {
				linenum++;
				// 解析头文件
				if (linenum == 1) {
					tableName = tablePrefix + filename.substring(3, 9);
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
					
					continue;
				}
				
				// 过滤尾行数据
				if (END.equals(line.substring(0, 2))) {
					break;
				}

				// 设置行键
				StringBuilder hsb = new StringBuilder();
				hsb.append(getIdentity(line.substring(172, 300))).append("|").append(setStartTime(line)).append("|")
					.append(filename).append("|").append(getSeqString(linenum));
				
				// 设置列值
				setValues(new String[] { line });

				// 入库
				addColumn(table, getIdentity(line.substring(172, 300)), getFamilyNames()[0], getColumns(), getValues(), null);

			}
			((HTable) table).flushCommits();

			LOG.info("end buildRecord");
		} catch (MasterNotRunningException e) {
			LOG.error("MasterNotRunningException:" + e.getMessage());
			throw new InterruptedException();
		} catch (ZooKeeperConnectionException e) {
			LOG.error("ZooKeeperConnectionException:" + e.getMessage());
			throw new InterruptedException();
		} catch (UnsupportedEncodingException e) {
			LOG.error("UnsupportedEncodingException:" + e.getMessage());
		} catch (IOException e) {
			LOG.error("IOException:" + e.getMessage());
		} finally {
			if (null != br) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return linenum - 1;
	}

	public String getIdentity(String cdr) {
		cdr = cdr.trim();
		if (cdr.length() > 3) {
			if (cdr.charAt(0) == '+') {
				cdr = cdr.substring(1);
			}
			if ("86".equals(cdr.subSequence(0, 2))) {
				cdr = cdr.substring(2);
			}
		}

		return cdr;
	}

	public String setStartTime(String cdr) throws NumberFormatException, UnsupportedEncodingException {
		String starttime = null;
		int cdr_type = Integer.parseInt(cdr.substring(10, 12));

		if (1 == cdr_type) {
			starttime = cdr.substring(1402, 1417);
		} else {
			starttime = cdr.substring(1432, 1447);
		}
		return starttime.trim();
	}

	public String getSeqString(int seq) {
		String seqString = Integer.toString(seq);
		StringBuilder ret = new StringBuilder();
		int shift = MAX_SEQ_LENGTH - seqString.length();
		for (int i = 0; i < shift; ++i) {
			ret.append("0");
		}
		ret.append(seqString);
		return ret.toString();
	}

}
