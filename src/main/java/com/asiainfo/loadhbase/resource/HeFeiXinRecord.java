package com.asiainfo.loadhbase.resource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;

public class HeFeiXinRecord extends Record {
	private static final String END = "90";

	@Override
	public boolean checkFileName(String name) {
		return name.startsWith("NMS") && name.length() == 18;
	}

	@Override
	public int buildRecord(String filename, BufferedReader br, Connection connection) throws InterruptedException {
		int lineNum = 0; // 行号
		String line = ""; // 每行数据
		Table table = null;
		String tableName = null;

		try {
			while ((line = br.readLine()) != null) {
				lineNum++;
				// 解析头文件
				if (lineNum == 1) {
					tableName = tableNamePrefix + filename.substring(3, 9);
					logger.info("currTableName:" + tableName);

					table = mapTable.get(tableName);
					if (table == null) {
						creatTable(tableName, getFamilys(), regions, connection);
						table = connection.getTable(TableName.valueOf(tableName));
						((HTable) table).setAutoFlushTo(false);
						((HTable) table).flushCommits();
						mapTable.put(tableName, table);
					}

					continue;
				}

				// 过滤尾行数据
				if (line.startsWith(END)) {
					break;
				}

				// 设置行键并入库
				StringBuffer rowKey = new StringBuffer();
				rowKey.append(getChargeNum(line)).append("|").append(setStartTime(line)).append("|").append(filename)
						.append("|").append(String.format("%09d", lineNum));
				addColumn(table, rowKey.toString(), getFamilys()[0], getColumns(), new String[] { line }, null);

			}
			((HTable) table).flushCommits();

			logger.info("end buildRecord");
		} catch (MasterNotRunningException e) {
			logger.error("MasterNotRunningException:" + e.getMessage());
			throw new InterruptedException();
		} catch (ZooKeeperConnectionException e) {
			logger.error("ZooKeeperConnectionException:" + e.getMessage());
			throw new InterruptedException();
		} catch (UnsupportedEncodingException e) {
			logger.error("UnsupportedEncodingException:" + e.getMessage());
		} catch (IOException e) {
			logger.error("IOException:" + e.getMessage());
		} finally {
			if (null != br) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return lineNum - 1;
	}

	public String getChargeNum(String cdr) {
		cdr = cdr.substring(172, 300).trim();
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
		int cdrType = Integer.parseInt(cdr.substring(10, 12));
		return cdrType==1 ? cdr.substring(1402, 1417).trim() : cdr.substring(1432, 1447).trim();
	}

}
