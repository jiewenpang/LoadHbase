package com.asiainfo.loadhbase;

import java.io.IOException;
import java.util.Map;

import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.asiainfo.loadhbase.handler.BaseHandler;

public class MainApp {
	
	public static void main(String[] args) throws Exception {
		// ��ȡ�����ļ���Ĭ��Ϊ��ǰĿ¼�£�Ҳ����ʹ�����в�������
		String appContextPath = "./applicationContext.xml";
		if (args.length > 0) {
			appContextPath = args[0];
		}

		// ��ʼ��
		FileSystemXmlApplicationContext appContext = new FileSystemXmlApplicationContext(appContextPath);
		Constant con = (Constant) appContext.getBean("property");
		try {
			con.initLog();
			con.initHbase();
		} catch (IOException e) {
			System.out.println("����:��ʼ���̶�����ʧ��");
			e.printStackTrace();
			appContext.close();
			return;
		}

		// ����ÿ��ҵ��
		BaseHandler.setAppContext(appContext);
		@SuppressWarnings("unchecked")
		Map<String, BaseHandler> beans = (Map<String, BaseHandler>) appContext.getBean("realHander");
        for(BaseHandler bean : beans.values()) {
        	bean.run();
        }
	}

}