package com.asiainfo.base;

import java.util.Date;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import com.asiainfo.bean.FileInfo;

public class CreatHDFSFile extends Thread{
	private FileSystem fs;
	private Path path;
	private List<FileInfo> fileinfos;
	private int start;
	private int end;
	private FSDataOutputStream os;
	public CreatHDFSFile(FileSystem fs,String path,List<FileInfo> fileinfos,int start,int end) {
		try {
			sleep(1);//睡1毫秒避免重名
			this.fs=fs;
			this.path=new Path(path+"/"+String.valueOf(new Date().getTime()));
//			System.out.println("写入文件:"+this.path.toString());
			this.fileinfos=fileinfos;
			this.os = this.fs.create(this.path, true, 1024);
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.start=start;
		this.end=end;
	}
	
	@Override
	public void run() {
		try {
			for(int i=this.start;i<this.end;i++){
				Text tx=new Text(this.fileinfos.get(i).getFileinfo());
				os.write(tx.getBytes(), 0, tx.getLength());
				os.write(Bytes.toBytes("\n"));
			}
			os.flush();
			os.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
