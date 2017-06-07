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

public class BOSSRecord extends Record {
	@Override
	public boolean checkFileName(String name) {
		return name.toLowerCase().startsWith("boss.");
	}

	@Override
	public int buildRecord(String filename, BufferedReader br, Connection connection) throws InterruptedException {
		int lineNum = 0;
		String line = "";
		StringBuffer sb = null;
		Table table = null;
		String tableName = null;

		try {
			while ((line = br.readLine()) != null) {
				sb = new StringBuffer(line);
				lineNum++;
				
				// 按照第一行建表
				if (lineNum == 1) {
					tableName = tableNamePrefix + sb.substring(139, 145);
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

				String telnum = null;
				String time = null;
				String area = null;
				String type = filename.substring(5, 7);
				if ("01/02/03/04/05".contains(type)) {
					area = sb.substring(125, 127);
					telnum = sb.substring(21, 45).trim();
					time = sb.substring(75, 89);
				} else if ("06/07".contains(type)) {
					area = sb.substring(57, 59);
					telnum = sb.substring(19, 43).trim();
					if ("06".equals(type)) {
						time = sb.substring(131, 145);
					} else if ("07".equals(type)) {
						time = sb.substring(146, 160);
					}
				} else if ("11/12/13/15".contains(type)) {
					area = sb.substring(149, 151);
					telnum = sb.substring(21, 45).trim();
					time = sb.substring(75, 89);
				}

				// 过滤部分地市
				if (lineNum == 2 && filterRegion.contains(area)) {
					logger.info("region:" + area);
					break;
				}

				// 设置行键并入库
				StringBuffer rowKey = new StringBuffer();
				rowKey.append(telnum).append("|").append(time).append("|").append(filename).append("|").append(lineNum);
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

}
