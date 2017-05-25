package com.asiainfo.bean;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTable;

public abstract class Base {
	protected String tabName;
	protected String[] regions;
	protected String rowKey;
	protected String[] familyNames; //Сазх
	protected String[] columns;
	protected String[] values;
	protected String filterregion;
	public Map<String, HTable> mapTable = new HashMap<String, HTable>();	//save billing month
	public void setFilterRegion(String filterregion) {
		this.filterregion = filterregion;
	}
	public abstract boolean checkFileName(String name);
	public abstract int buildRecord(HTable table,String filename,BufferedReader br) throws Exception;
	
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
	public String[] getValues() {
		return values;
	}
	public void setValues(String[] values) {
		this.values = values;
	}
	public String getRowKey() {
		return rowKey;
	}
	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}
	public String getTabName() {
		return tabName;
	}
	public void setTabName(String tabName) {
		this.tabName = tabName;
	}
	public String[] getRegions() {
		return regions;
	}
	public void setRegions(String[] regions) {
		this.regions = regions;
	}
	

}
