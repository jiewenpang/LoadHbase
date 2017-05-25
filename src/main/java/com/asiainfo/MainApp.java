package com.asiainfo;

import java.io.FileInputStream;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.asiainfo.base.CreatHDFSFile;
import com.asiainfo.base.DealClass;
import com.asiainfo.base.DealMap;
import com.asiainfo.base.HbaseHelper;
import com.asiainfo.bean.Base;
import com.asiainfo.bean.FileInfo;
import com.asiainfo.tool.FtpTools;

public class MainApp {
	static final Log LOG = LogFactory.getLog(MainApp.class);
	static final SimpleDateFormat sf=new SimpleDateFormat("yyyyMM");
	public static Properties rs;
	
	
	public static void main(String[] args) throws Exception {
		if(args.length==0){
//			args=new String[]{"D:\\workdir\\QZDE\\QzdBase\\src\\ftp.properties"};
			System.out.println("please input config file path!");
			return;
		}
		System.out.println("process begin....");
		rs = new Properties();
		rs.load(new FileInputStream(args[0]));
		String tableNamePrefix=rs.getProperty("tableNamePrefix");
		String dealcls=rs.getProperty("dealCls");
		String[] regions=rs.getProperty("splitKeyPrefixes", "").split(",");
		String[] familys=rs.getProperty("tabFamily", "").split(",");
		int tdnum=Integer.valueOf(rs.getProperty("hdfsFileTDNum"));
		String input=rs.getProperty("input");
		String inputlarge=rs.getProperty("inputlarge");
		String cmd = rs.getProperty("cmd");
		Integer maptotalnum = Integer.valueOf(rs.getProperty("maptotalnum", "600"));	//map������
		//1Ϊ��ftp��ʽ��ȡ�ļ��б�2Ϊ��ssh��ʽ��ȡ�ļ��б�
		String type = rs.getProperty("type", "2");
		//ssh���ӻ�ȡ�ļ��б�ʱʹ�õĶ˿ں�
		String icfgport = rs.getProperty("icfgport","22");
		//Mapperÿ̨���ӵ������߳����������CPU�߼���Ԫ-1������ҲҪ����FTP��������
		int maptasks = Integer.valueOf(rs.getProperty("maptasks"));
		Path inputpath=new Path(input);
        FileSystem fs= FileSystem.get(HbaseHelper.conf); 
        
        //�Ƿ���HDFS
        int isinputhdfs = Integer.valueOf(rs.getProperty("isinputhdfs","1"));
        if ( isinputhdfs == 1 ){
	        if (!fs.exists(inputpath)) {
				fs.mkdirs(inputpath);
				fs.mkdirs(inputpath, new FsPermission("777"));
			}else{
				System.out.println("input file existed!clean...");
				fs.delete(inputpath,true);
				System.out.println("clean finish...create folder");
				fs.mkdirs(inputpath);
				fs.mkdirs(inputpath, new FsPermission("777"));
			}
        }
        System.out.println("read config finish....create ftp file list begin...");
		//ftp
		String[] ftps=rs.getProperty("ftp","").split(",");
		int filenum=0;
		List<FileInfo> fileinfolist = new ArrayList<FileInfo>();
		Long totalsize = 0l; //�����ļ��ܴ�С
		for(String ftpc:ftps){//ftp��hdfs�ļ�ϵͳ�д����ļ�
			String[] ftpdesc=ftpc.split(":");
			if(ftpdesc.length!=5){
				LOG.error("ftp config error!"+ftpc);
				//���ô���
				continue;
			}
			//ftpdesc[0], ftpdesc[2], ftpdesc[3], buf.insert(3, ftpdesc[4]).toString()
			
			StringBuffer buf = new StringBuffer();
			buf.append(cmd);
			int index = buf.indexOf("|");
			String cmds = buf.insert(index, ftpdesc[4]).toString();
			FtpTools ftp= FtpTools.newInstance(ftpdesc[0],Integer.valueOf(ftpdesc[1]),ftpdesc[2],ftpdesc[3],ftpdesc[4]);
			try {
//				if(ftp.connectServer()){
				System.out.println("ftpinfo->"+
						ftpdesc[0]+":"+
						Integer.valueOf(ftpdesc[1])+":"+
						ftpdesc[4] + ",cmds:" + cmds);
				if(ftp.connectServer(Integer.valueOf(type), Integer.valueOf(icfgport))){	//typeȡֵ˵����1Ϊftp���ӷ�ʽ ��2Ϊssh���ӷ�ʽ
					totalsize += ftp.getMapList((Base)Class.forName(dealcls).newInstance(), cmds, fileinfolist);

				}else{
					LOG.error("login fail!"+ftpc);
					return;
				}
			} catch (Exception e) {
				e.printStackTrace();
				LOG.error("FTP error!", e);
				return;
			}
		}
		filenum = fileinfolist.size();
		Long avgfilesize = totalsize/maptotalnum; //ÿ��map������ļ��ܴ�С
		System.out.println("ftp list finish:" + filenum + ",avgfilesize:" + avgfilesize +"");
		if ( filenum == 0 )
		{
			System.out.println("task finish,quit!");
			return;
		}
		
		if ( isinputhdfs == 1 ){
			InputHDFS(tdnum, input, fs, fileinfolist, avgfilesize, 2);
		}
			
		String yyyymm=sf.format(new Date());
		System.out.println("create table begin:"+tableNamePrefix+yyyymm);
		
		if(regions.length>1 || !"".equals(regions[0])){
			byte[][] regs=new byte[regions.length][];
			for(int j=0;j<regions.length;j++){
				regs[j]=Bytes.toBytes(regions[j]);
			}
			HbaseHelper.creatTable(tableNamePrefix+yyyymm, familys,regs);
		}else{
			HbaseHelper.creatTable(tableNamePrefix+yyyymm, familys,null);
		}
		System.out.println("create table end.");
		
		if ( isinputhdfs == 1 )
		{
			System.out.println("config Job begin..");
			JobConf conf=new JobConf(HbaseHelper.conf);
			conf.setNumMapTasks(filenum/tdnum);
			conf.set("output", tableNamePrefix+yyyymm);
			conf.set("dealCls", dealcls);
			conf.set("region", rs.getProperty("splitKeyPrefixes", ""));
			conf.set("family",rs.getProperty("tabFamily", ""));
			
			conf.set("filterregion",rs.getProperty("filterregion", ""));
			
			conf.set("column", rs.getProperty("column","")); //������
			conf.set("ischgport", rs.getProperty("ischgport","0")); //ftp��ȡ�ļ��Ƿ񻻶˿ڣ�����ǣ���ʹ��21�˿ڻ�ȡ�ļ�������ʹ�����õĶ˿ڻ�ȡ�ļ�
			conf.set("isbakinput", rs.getProperty("isbakinput","0"));	//�Ƿ񱸷������ļ���1��ʾ������ɺ󱸷ݵ�����Ŀ¼ͬ����bak133Ŀ¼��0��ʾ������ɺ�ֱ��ɾ�������ļ�
			conf.set("inputlarge", inputlarge);
			conf.set("largesize", rs.getProperty("largesize", "1073741824"));
			
			conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 150*tdnum);//Integer.MAX_VALUE;
			conf.setLong("mapreduce.input.fileinputformat.split.minsize", 1L);
			conf.setLong("mapred.min.split.size", 1L);
			conf.setInt("mapred.task.timeout", 3600000);
			conf.setLong("mapred.tasktracker.map.tasks.maximum", maptasks);//Mapperÿ̨���ӵ������߳����������CPU�߼���Ԫ-1������ҲҪ����FTP��������
			conf.setInt("mapred.max.map.failures.percent", 1);//mapperʧ�����Դ�����Ĭ����4.��׼�������л��ӹҵ�,������ͨ���˻ᵼ�²���ʧ�ܣ��������ô˲�������������ʧ�ܴ���̫��ᵼ������Jobʧ�ܡ�
			//conf.set("mapred.map.child.java.opts", "-agentpath:/usr/lib/transwarp-manager/agent/lib/native/libagent.so -Xmx8192m");  //����Ĭ��map �Ѵ�С,�ڲ������ļ�ʱ��ֹ�ڴ����
			//conf.setInt("mapreduce.map.memory.mb", 5120);
			//conf.set("yarn.nodemanager.vmem-check-enabled", "false");
			//conf.set("yarn.nodemanager.pmem-check-enabled", "false");
			//conf.setDouble("yarn.nodemanager.vmem-pmem-ratio", 2.1);
			conf.set("detailoutputdir", rs.getProperty("detailoutputdir"));
			conf.set("bakdir", rs.getProperty("bakdir", "bak_133"));
			Job job=Job.getInstance(conf);
			job.setJobName("QJDBASE_JOB_"+tableNamePrefix+yyyymm);
			job.setMapperClass(DealMap.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setOutputFormatClass(NullOutputFormat.class);
			job.setNumReduceTasks(0);
			FileInputFormat.addInputPath(job, inputpath);
			System.out.println("config HbaseJOB...");
			
			long sttime=System.currentTimeMillis();
	        job.setJarByClass(MainApp.class);
	        System.out.println("excute Job begin..");
	        
			System.out.println((job.waitForCompletion(true)?"excute task successfully!":"excute task fail!"));
			System.out.println("clean begin...");
			fs.delete(inputpath,true);
			long endtime=System.currentTimeMillis();
			System.out.println("file list:"+filenum+",all task finish,total time:"+(endtime-sttime) /1000 +"s,quit...");
		}
		else{
			long sttime=System.currentTimeMillis();
			DealClass Deal = new DealClass();
			Deal.setup(tableNamePrefix+yyyymm,dealcls,rs.getProperty("splitKeyPrefixes", ""),rs.getProperty("tabFamily", ""),rs.getProperty("column",""),rs.getProperty("filterregion", ""),rs.getProperty("ischgport","0"));
			Deal.map(fileinfolist);
			Deal.cleanup();
			long endtime=System.currentTimeMillis();
			System.out.println("file list:"+filenum+",all task finish,total time:"+(endtime-sttime) /1000 +"s,quit...");
		}
		
		
		
	}


	private static void InputHDFS(int tdnum, String input, FileSystem fs,
			List<FileInfo> fileinfos, Long avgfilesize, int itype) throws InterruptedException {
		ExecutorService pool = Executors.newFixedThreadPool(50);
		
		if ( itype == 1 )
		{
			Long currentfilesize = 0l;//��ǰ�ļ��ܴ�С
			int beginindex = 0;
			int endindex = 0;
			CreatHDFSFile td = null;
			for(int i = beginindex, len = fileinfos.size(); i < len; i++){
				currentfilesize += fileinfos.get(i).getSize();
				if(avgfilesize <= currentfilesize){
					endindex = i + 1;
					td = new CreatHDFSFile(fs, input, fileinfos, beginindex, endindex);
					beginindex = endindex;
					currentfilesize = 0l;
					pool.execute(td);
				}
			}
			if(endindex != fileinfos.size()){
				td = new CreatHDFSFile(fs, input, fileinfos, beginindex, fileinfos.size());
				pool.execute(td);
			}
		}
		else
		{
			if (fileinfos.size() - tdnum < 0)
			{
				  CreatHDFSFile td = new CreatHDFSFile(fs, input, fileinfos, 0, fileinfos.size());
				  pool.execute(td);
			}
			else
			{
			  for (int i = 0; i < fileinfos.size(); i += tdnum)
			  {
			    CreatHDFSFile td;
			    if (i + tdnum <= fileinfos.size()) {
			      td = new CreatHDFSFile(fs, input, fileinfos, i, i + tdnum);
			    } else {
			      td = new CreatHDFSFile(fs, input, fileinfos, i, fileinfos.size());
			    }
			    pool.execute(td);
			  }
			}
		}
		pool.shutdown();
		while(!pool.awaitTermination(10, TimeUnit.SECONDS));
	}
}
