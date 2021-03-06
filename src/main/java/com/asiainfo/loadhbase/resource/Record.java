package com.asiainfo.loadhbase.resource;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
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
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Record implements Writable {
	protected static final Logger logger = LoggerFactory.getLogger(Record.class);
	protected String name;
	protected String tableNamePrefix;
	protected String filterRegion;
	protected String[] familys;
	protected String[] columns;
	protected String[] regions;
	protected Map<String, Table> mapTable = new HashMap<String, Table>();
	
	// 以下参数不需要序列化，仅利于按业务进行配置化
	protected String ftpInfo;
	protected String inputHdfsPath;
	protected String inputBakPath;
	protected String detailOutputPath;
	protected String maxFileHandlePath;
	
	public abstract boolean checkFileName(String name);
	public abstract int buildRecord(String filename, BufferedReader br, Connection connection) throws Exception;

	@Override
	public void readFields(DataInput arg0) throws IOException {
    	Text tx = new Text();
    	ArrayWritable aw = new ArrayWritable(Text.class);

    	tx.readFields(arg0);
    	tableNamePrefix = tx.toString();
    	
    	tx.readFields(arg0);
    	filterRegion = tx.toString();
    	
    	aw.readFields(arg0);
    	familys = aw.toStrings();

    	aw.readFields(arg0);
    	columns = aw.toStrings();
    	
    	aw.readFields(arg0);
    	regions = aw.toStrings();

	}
	
	@Override
	public void write(DataOutput arg0) throws IOException {
		new Text(tableNamePrefix).write(arg0);
		new Text(filterRegion).write(arg0);
		GetArrayText(familys).write(arg0);
		GetArrayText(columns).write(arg0);
		GetArrayText(regions).write(arg0);
	}
	
	private ArrayWritable GetArrayText(String[] strings) {
    	Text[] values = new Text[strings.length];
        for (int i = 0; i < strings.length; i++) {
          values[i] = new Text(strings[i]);
        }
        
    	return new ArrayWritable(Text.class, values);
	}

	public static boolean creatTable(String tableName, String[] family, String[] regions, Connection connection)
			throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));

		byte[][] regs = null;
		if (regions.length > 1 || !"".equals(regions[0])) {
			regs = new byte[regions.length][];
			for (int j = 0; j < regions.length; j++) {
				regs[j] = Bytes.toBytes(regions[j]);
			}
		}
		
		for (int i = 0; i < family.length; i++) {
			HColumnDescriptor cl = new HColumnDescriptor(family[i]);
			cl.setMaxVersions(1);
			cl.setBloomFilterType(BloomType.ROW);
			cl.setCompressionType(Compression.Algorithm.SNAPPY);
			desc.addFamily(cl);
		}
		
		try {
			connection.getAdmin().createTable(desc, regs);
			logger.info("create table " + tableName + " Success!");
		} catch (org.apache.hadoop.hbase.TableExistsException e){
			logger.info("table " + tableName + " Exists!");
		}
		
		return true;
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
				logger.info("put flushtotable exception,wait 300ms and try again:" + e.getMessage());
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
		logger.info(falilyName + ":" + columnName + " is deleted!");
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
		logger.info(tableName + " is deleted!");
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getTableNamePrefix() {
		return tableNamePrefix;
	}

	public void setTableNamePrefix(String tableNamePrefix) {
		this.tableNamePrefix = tableNamePrefix;
	}

	public String getFilterRegion() {
		return filterRegion;
	}

	public void setFilterRegion(String filterRegion) {
		this.filterRegion = filterRegion;
	}

	public String[] getFamilys() {
		return familys;
	}

	public void setFamilys(String[] familys) {
		this.familys = familys;
	}

	public String[] getColumns() {
		return columns;
	}

	public void setColumns(String[] columns) {
		this.columns = columns;
	}

	public String[] getRegions() {
		return regions;
	}

	public void setRegions(String[] regions) {
		this.regions = regions;
	}

	public String getFtpInfo() {
		return ftpInfo;
	}

	public void setFtpInfo(String ftpInfo) {
		this.ftpInfo = ftpInfo;
	}

	public String getInputHdfsPath() {
		return inputHdfsPath;
	}

	public void setInputHdfsPath(String inputHdfsPath) {
		this.inputHdfsPath = inputHdfsPath;
	}

	public String getInputBakPath() {
		return inputBakPath;
	}

	public void setInputBakPath(String inputBakPath) {
		this.inputBakPath = inputBakPath;
	}

	public String getDetailOutputPath() {
		return detailOutputPath;
	}

	public void setDetailOutputPath(String detailOutputPath) {
		this.detailOutputPath = detailOutputPath;
	}

	public String getMaxFileHandlePath() {
		return maxFileHandlePath;
	}

	public void setMaxFileHandlePath(String maxFileHandlePath) {
		this.maxFileHandlePath = maxFileHandlePath;
	}

	public Map<String, Table> getMapTable() {
		return mapTable;
	}

	public void setMapTable(Map<String, Table> mapTable) {
		this.mapTable = mapTable;
	}

}
