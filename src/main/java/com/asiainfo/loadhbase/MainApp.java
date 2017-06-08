package com.asiainfo.loadhbase;

import java.io.IOException;

import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.asiainfo.loadhbase.handler.BaseHandler;

public class MainApp {
	
	public static void main(String[] args) throws Exception {
		// 读取配置文件，默认为当前目录下，也可以使用运行参数配置
		String appContextPath = "./applicationContext.xml";
		if (args.length > 0) {
			appContextPath = args[0];
		}

		// 初始化
		FileSystemXmlApplicationContext appContext = new FileSystemXmlApplicationContext(appContextPath);
		Constant con = (Constant) appContext.getBean("property");
		try {
			con.initLog();
			con.initHbase();
		} catch (IOException e) {
			System.out.println("错误:初始化固定参数失败");
			e.printStackTrace();
			appContext.close();
			return;
		} 

		// 启动每个业务
		BaseHandler.setHbaseConfiguration(Constant.getHadoopConfig());
		BaseHandler.setConnection(Constant.getConnection());
		BaseHandler bean = (BaseHandler) appContext.getBean("realHander");
		bean.run();
		appContext.close();
	}

}