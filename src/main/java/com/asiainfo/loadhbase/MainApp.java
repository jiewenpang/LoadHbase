package com.asiainfo.loadhbase;

import java.io.IOException;
import java.util.Map;

import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.asiainfo.loadhbase.handler.BaseHandler;

public class MainApp {
	
	public static FileSystemXmlApplicationContext context=null;
	public static void main(String[] args) throws Exception {
		// ��ȡ�����ļ���Ĭ��Ϊ��ǰĿ¼�£�Ҳ����ʹ�����в�������
		String appContextPath = "./applicationContext.xml";
		if (args.length > 0) {
			appContextPath = args[0];
		}

		// ��ʼ��
		BaseHandler.setAppContext(new FileSystemXmlApplicationContext(appContextPath));
		Constant con = (Constant) BaseHandler.getAppContext().getBean("property");
		try {
			con.initLog();
			con.initHbase();
		} catch (IOException e) {
			System.out.println("����:��ʼ���̶�����ʧ��");
			e.printStackTrace();
			return;
		}

		// ����ÿ��ҵ��
		Map<String, BaseHandler> beans = BaseHandler.getAppContext().getBeansOfType(BaseHandler.class);
        for(BaseHandler bean : beans.values()) {
        	bean.run();
        }
        
	}
}