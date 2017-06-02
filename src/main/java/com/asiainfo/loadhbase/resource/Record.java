package com.asiainfo.loadhbase.resource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Record {
	protected static final Logger logger = LoggerFactory.getLogger(Record.class);
	protected String tablePrefix;
	protected String[] regions;
	protected String[] familyNames;
	protected String[] columns;
	protected String filterRegion;
	public Map<String, Table> mapTable = new HashMap<String, Table>();

	public abstract boolean checkFileName(String name);

	public abstract int buildRecord(String filename, BufferedReader br, Connection connection) throws Exception;

	public String[] getRegions() {
		return regions;
	}

	public void setRegions(String[] regions) {
		this.regions = regions;
	}

	public String[] getFamilyNames() {
		return familyNames;
	}

	public void setFamilyNames(String[] familyNames) {
		this.familyNames = familyNames;
	}

	public String[] getColumns() {
		return columns;
	}

	public void setColumns(String[] columns) {
		this.columns = columns;
	}

	public String getFilterRegion() {
		return filterRegion;
	}

	public void setFilterRegion(String filterRegion) {
		this.filterRegion = filterRegion;
	}

	public static boolean creatTable(String tableName, String[] family, byte[][] region, Connection connection)
			throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		boolean flag = false;
		Admin admin = connection.getAdmin();
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
		for (int i = 0; i < family.length; i++) {
			HColumnDescriptor cl = new HColumnDescriptor(family[i]);
			cl.setMaxVersions(1);
			cl.setBloomFilterType(BloomType.ROW);
			cl.setCompressionType(Compression.Algorithm.SNAPPY);
			desc.addFamily(cl);
		}
		if (admin.tableExists(desc.getTableName())) {
			logger.info("table " + tableName + " Exists!");
		} else {
			if (region != null)
				admin.createTable(desc, region);
			else
				admin.createTable(desc);
			logger.info("create table " + tableName + " Success!");
			flag = true;
		}
		return flag;
	}

	public static void addColumn(Table table, String rowKey, String family, String[] columns, String[] values,
			String oType) throws UnsupportedEncodingException, IOException {
		Put put = new Put(Bytes.toBytes(rowKey));

		for (int i = 0; i < columns.length; i++) {
			if (null == oType) {
				put.addColumn(Bytes.toBytes(family), Bytes.toBytes(columns[i]), Bytes.toBytes(values[i]));
			} else {
				put.addColumn(Bytes.toBytes(family), Bytes.toBytes(columns[i]), values[i].getBytes(oType));
			}
		}

		flushToTable(table, put, 5);
	}

	public static void flushToTable(Table table, Put put, int times) {
		if (times <= 0) {
			logger.error("Heavy insert failed many times!");
		} else {
			try {
				table.put(put);
			} catch (Exception e) {
				logger.info("put flushtotable exception:" + e.getMessage());
				logger.info("flushtotable wait 300ms");
				try {
					Thread.sleep(300);
				} catch (InterruptedException e1) {
					logger.info("sleep exception:" + e1.getMessage());
					e1.printStackTrace();
				}
				times = times - 1;
				flushToTable(table, put, times);
			}
		}
	}

	public static void deleteColumn(String tableName, String rowKey, String falilyName, String columnName,
			Connection connection) throws IOException {
		Table table = connection.getTable(TableName.valueOf(tableName));
		Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
		deleteColumn.addColumns(Bytes.toBytes(falilyName), Bytes.toBytes(columnName));
		table.delete(deleteColumn);
		logger.info(falilyName + ":" + columnName + "is deleted!");
	}

	public static void deleteAllColumn(String tableName, String rowKey, Connection connection) throws IOException {
		Table table = connection.getTable(TableName.valueOf(tableName));
		Delete deleteAll = new Delete(Bytes.toBytes(rowKey));
		table.delete(deleteAll);
		logger.info("all columns are deleted!");
	}

	public static void deleteTable(String tableName, Connection connection) throws IOException {
		Admin admin = connection.getAdmin();
		admin.disableTables(tableName);
		admin.deleteTables(tableName);
		logger.info(tableName + "is deleted!");
	}

}
