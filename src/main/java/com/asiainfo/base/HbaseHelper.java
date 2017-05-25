package com.asiainfo.base;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseHelper {
	static final Log LOG = LogFactory.getLog(HbaseHelper.class);
	public static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
        String hbasehome=System.getenv("HBASE_HOME");
        String hadoophome=System.getenv("HADOOP_HOME");
        if(null==hbasehome || "".equals(hbasehome)){
        	LOG.info("��������û������HBASE_HOME��Ĭ��Ϊ��/usr/lib/hbase");
        	hbasehome="/usr/lib/hbase";
        }
        if(null==hadoophome || "".equals(hadoophome)){
        	LOG.info("��������û������HADOOP_HOME,Ĭ��Ϊ��/usr/lib/hadoop");
        	hadoophome="/usr/lib/hadoop";
        }
        hbasehome+="/conf/hbase-site.xml";
        conf.addResource(new Path(hbasehome));
        
        String coresitexml=hadoophome+"/etc/hadoop/core-site.xml";
        String hdfssitexml=hadoophome+"/etc/hadoop/hdfs-site.xml";
        String mapredsitexml=hadoophome+"/etc/hadoop/mapred-site.xml";
        String yarnsitexml=hadoophome+"/etc/hadoop/yarn-site.xml";
        
        conf.addResource(new Path(coresitexml));
        conf.addResource(new Path(hdfssitexml));
        conf.addResource(new Path(mapredsitexml));
        conf.addResource(new Path(yarnsitexml));
        
        //���mapreduce�޷�ɨ�赽hdfs����Ŀ¼����,hadoop-common-x.jar δ��������
        //The FileSystem for hdfs: uris.
        //conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        //The FileSystem for file: uris.
        //conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        
//        System.out.println("zook:"+conf.get("hbase.zookeeper.quorum"));
//        conf.set("hbase.zookeeper.quorum", MainApp.rs.getProperty("Hip"));
//        conf.set("hbase.zookeeper.property.clientPort",  MainApp.rs.getProperty("Hport"));
        //conf.set("hbase.zookeeper.quorum", "localhost");
    }

    /**
     * 
     * @param tableName ��
     * @param family �����б�
     * @param region ����
     * @return
     * @throws Exception
     */
    public static boolean creatTable(String tableName, String[] family,byte[][] region)
    throws MasterNotRunningException ,ZooKeeperConnectionException ,IOException{
    	boolean flag=false;
			HBaseAdmin admin = new HBaseAdmin(conf);
			HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
			
			for (int i = 0; i < family.length; i++) {
				HColumnDescriptor cl=new HColumnDescriptor(family[i]);
				cl.setMaxVersions(1);
				cl.setBloomFilterType(BloomType.ROW);
				cl.setCompressionType(Compression.Algorithm.SNAPPY);
			    desc.addFamily(cl);
			}
			if (admin.tableExists(tableName)) {
			    LOG.info("table "+tableName+" Exists!");
			} else {
				if(region!=null)
					admin.createTable(desc,region);
				else
					admin.createTable(desc);
			    LOG.info("create table "+tableName+" Success!");
			    flag=true;
			}
        return flag;
    }

    /**
     * ��������
     * @param table HTable����
     * @param rowKey �м�
     * @param tableName ����
     * @param family  ����
     * @param columns ����������
     * @param values ��ֵ����
     * @param oType ���뷽ʽ����null����UTF-8������GBK��
     * @throws IOException
     */
    public static void addData(HTable table,
    						  String rowKey, 
    						  String family,
    						  String[] columns, 
    						  String[] values,
    						  Object ...oType)  throws UnsupportedEncodingException, IOException{
        Put put = new Put(Bytes.toBytes(rowKey));// ����rowkey
//        HTable table = new HTable(conf, Bytes.toBytes(tableName));// HTabel�������¼��صĲ�������ɾ�Ĳ��
			//HColumnDescriptor[] columnFamilies = table.getTableDescriptor() // ��ȡ���е�����
			        //.getColumnFamilies();
			//table.setWriteBufferSize(10 * 1024 * 1024);
			//for (int i = 0; i < columnFamilies.length; i++) {
			   // String familyName = columnFamilies[i].getNameAsString(); // ��ȡ������
			   // if (familyName.equals(family)) { // ������put����
			        for (int j = 0; j < columns.length; j++) {
			        	if ( null == oType ){
			        		put.add(Bytes.toBytes(family),
			                    Bytes.toBytes(columns[j]), Bytes.toBytes(values[j]));
			        	}else{
			        		put.add(Bytes.toBytes(family),
			                        Bytes.toBytes(columns[j]), values[j].getBytes((String)oType[0]));
			        	}
			        }
			    //}
//            if (familyName.equals("author")) { // author����put����
//                for (int j = 0; j < column2.length; j++) {
//                    put.add(Bytes.toBytes(familyName),
//                            Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));
//                }
//            }
			//}
//			table.put(put);
        	flushtotable(table, put, 5);
    }
    
    public static void flushtotable(HTable table, Put put, int times){
    	if(times <= 0){
    		LOG.error("Heavy insert failed many times!");
    	}else{
	    	try {
	    		table.put(put);
			} catch (Exception e) {
				LOG.info("put flushtotable exception:" + e.getMessage());
				LOG.info("flushtotable wait 300ms");
				try {
					Thread.sleep(300);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					LOG.info("sleep exception:" + e1.getMessage());
					e1.printStackTrace();
				}
				times = times - 1;
				flushtotable(table, put, times);
			}
    	}
    }

    /*
     * ����rwokey��ѯ
     * 
     * @rowKey rowKey
     * 
     * @tableName ����
     */
    public static Result getResult(String tableName, String rowKey)
            throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        HTable table = new HTable(conf, Bytes.toBytes(tableName));// ��ȡ��
        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            LOG.info("family:" + Bytes.toString(kv.getFamily()));
            LOG.info("qualifier:" + Bytes.toString(kv.getQualifier()));
            LOG.info("value:" + Bytes.toString(kv.getValue()));
            LOG.info("Timestamp:" + kv.getTimestamp());
            LOG.info("-------------------------------------------");
        }
        return result;
    }

    /**
     * ������ѯhbase��
     * 
     * @tableName ����
     */
    public static void getResultScann(String tableName) throws IOException {
        Scan scan = new Scan();
        ResultScanner rs = null;
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (KeyValue kv : r.list()) {
                    LOG.info("row:" + Bytes.toString(kv.getRow()));
                    LOG.info("family:"
                            + Bytes.toString(kv.getFamily()));
                    LOG.info("qualifier:"
                            + Bytes.toString(kv.getQualifier()));
                    System.out
                            .println("value:" + Bytes.toString(kv.getValue()));
                    LOG.info("timestamp:" + kv.getTimestamp());
                    System.out
                            .println("-------------------------------------------");
                }
            }
        } finally {
            rs.close();
        }
    }

    /**
     * ������ѯhbase��
     * 
     * @tableName ����
     */
    public static void getResultScann(String tableName, String start_rowkey,
            String stop_rowkey) throws IOException {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(start_rowkey));
        scan.setStopRow(Bytes.toBytes(stop_rowkey));
        ResultScanner rs = null;
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (KeyValue kv : r.list()) {
                    LOG.info("row:" + Bytes.toString(kv.getRow()));
                    LOG.info("family:"
                            + Bytes.toString(kv.getFamily()));
                    LOG.info("qualifier:"
                            + Bytes.toString(kv.getQualifier()));
                    System.out
                            .println("value:" + Bytes.toString(kv.getValue()));
                    LOG.info("timestamp:" + kv.getTimestamp());
                    System.out
                            .println("-------------------------------------------");
                }
            }
        } finally {
            rs.close();
        }
    }

    /**
     * ��ѯ���е�ĳһ��
     * 
     * @tableName ����
     * 
     * @rowKey rowKey
     */
    public static void getResultByColumn(String tableName, String rowKey,
            String familyName, String columnName) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName)); // ��ȡָ������������η���Ӧ����
        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            LOG.info("family:" + Bytes.toString(kv.getFamily()));
            System.out
                    .println("qualifier:" + Bytes.toString(kv.getQualifier()));
            LOG.info("value:" + Bytes.toString(kv.getValue()));
            LOG.info("Timestamp:" + kv.getTimestamp());
            LOG.info("-------------------------------------------");
        }
    }

    /**
     * ���±��е�ĳһ��
     * 
     * @tableName ����
     * 
     * @rowKey rowKey
     * 
     * @familyName ������
     * 
     * @columnName ����
     * 
     * @value ���º��ֵ
     */
    public static void updateTable(String tableName, String rowKey,
            String familyName, String columnName, String value)
            throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName),
                Bytes.toBytes(value));
        table.put(put);
        LOG.info("update table Success!");
    }

    /**
     * ��ѯĳ�����ݵĶ���汾
     * 
     * @tableName ����
     * 
     * @rowKey rowKey
     * 
     * @familyName ������
     * 
     * @columnName ����
     */
    public static void getResultByVersion(String tableName, String rowKey,
            String familyName, String columnName) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
        get.setMaxVersions(5);
        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            LOG.info("family:" + Bytes.toString(kv.getFamily()));
            LOG.info("qualifier:" + Bytes.toString(kv.getQualifier()));
            LOG.info("value:" + Bytes.toString(kv.getValue()));
            LOG.info("Timestamp:" + kv.getTimestamp());
            LOG.info("-------------------------------------------");
        }
        /*
         * List<?> results = table.get(get).list(); Iterator<?> it =
         * results.iterator(); while (it.hasNext()) {
         * LOG.info(it.next().toString()); }
         */
    }

    /**
     * ɾ��ָ������
     * 
     * @tableName ����
     * 
     * @rowKey rowKey
     * 
     * @familyName ������
     * 
     * @columnName ����
     */
    public static void deleteColumn(String tableName, String rowKey,
            String falilyName, String columnName) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
        deleteColumn.deleteColumns(Bytes.toBytes(falilyName),
                Bytes.toBytes(columnName));
        table.delete(deleteColumn);
        LOG.info(falilyName + ":" + columnName + "is deleted!");
    }

    /**
     * ɾ��ָ������
     * 
     * @tableName ����
     * 
     * @rowKey rowKey
     */
    public static void deleteAllColumn(String tableName, String rowKey)
            throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Delete deleteAll = new Delete(Bytes.toBytes(rowKey));
        table.delete(deleteAll);
        LOG.info("all columns are deleted!");
    }

    /**
     * ɾ����
     * 
     * @tableName ����
     */
    public static void deleteTable(String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        LOG.info(tableName + "is deleted!");
    }
}
