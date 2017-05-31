package com.asiainfo.loadhbase;

import java.io.IOException;
import java.util.Map;

import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.asiainfo.loadhbase.handler.BaseHandler;

public class MainApp {
	
	public static FileSystemXmlApplicationContext context=null;
	public static void main(String[] args) throws Exception {
		// 读取配置文件，默认为当前目录下，也可以使用运行参数配置
		String appContextPath = "./applicationContext.xml";
		if (args.length > 0) {
			appContextPath = args[0];
		}

		// 初始化
		BaseHandler.setAppContext(new FileSystemXmlApplicationContext(appContextPath));
		Constant con = (Constant) BaseHandler.getAppContext().getBean("property");
		try {
			con.initLog();
			con.initHbase();
		} catch (IOException e) {
			System.out.println("错误:初始化固定参数失败");
			e.printStackTrace();
			return;
		}

		// 启动每个业务
		Map<String, BaseHandler> beans = BaseHandler.getAppContext().getBeansOfType(BaseHandler.class);
        for(BaseHandler bean : beans.values()) {
        	bean.run();
        }
        
	}
}