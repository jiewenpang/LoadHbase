package com.asiainfo.bean.list;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import com.asiainfo.base.HbaseHelper;
import com.asiainfo.bean.Base;

/**
 * �ͷ���
 * 
 * @author Administrator
 *
 */
public class HeFeiXinDeal extends Base {
	public static final Log LOG = LogFactory.getLog(HeFeiXinDeal.class);
	private String _fileName;   //�ļ�����
	public static final String END="90";
	private static int MAX_SEQ_LENGTH = 9;
	private static final String TABLENAME_PREFIX = "NMS_";
	private String cdr;
	private String mobNo;  //���ͷ��û���ʶ
	private String startTime; //�ػ���ʼʱ��

	@Override
	public boolean checkFileName(String name) {
		boolean flag = false;
		if (name.startsWith("NMS")) {
			flag = true;
		}
		return flag;
	}	
	@Override
	public int buildRecord(HTable table,String filename,BufferedReader br) throws InterruptedException {
		int linenum = 0; // �к�
		String line = ""; // ÿ������
		LOG.info("buildRecord");
		HTable tab = table;
		try {
			while ((line = br.readLine()) != null) {
				linenum++;
				// ����ͷ�ļ�
				if (linenum == 1) {
					String tbname = new String(table.getTableName());
					String yearMonth = filename.substring(3, 9);
					set_fileName(filename); // �����ļ���
					if (tbname.endsWith(yearMonth)) {
						continue;
					} else {
						tbname = TABLENAME_PREFIX + yearMonth;
					}
					if(mapTable.containsKey(tbname) == false) {
						if (getRegions().length > 1 || !"".equals(getRegions()[0])) {
							byte[][] regs = new byte[getRegions().length][];
							for (int j = 0; j < getRegions().length; j++) {
								regs[j] = Bytes.toBytes(getRegions()[j]);
							}
							HbaseHelper.creatTable(tbname, getFamilyNames(), regs);
						} else {
							HbaseHelper.creatTable(tbname, getFamilyNames(), null);
						}
						tab = new HTable(HbaseHelper.conf, Bytes.toBytes(tbname));
						tab.setAutoFlush(false);
						tab.flushCommits();
						mapTable.put(tbname, tab);
					}else{
						tab = mapTable.get(tbname);
					}
					
					// ���ñ���
					setTabName(tbname);
					continue;
				}
				//����β��
				if(END.equals(line.substring(0, 2))){
					break;
				}
				
				byte[] body=line.getBytes("GBK");  //���ı�����ת���ֽ�
				this.cdr=new String(body);
				this.mobNo=getIdentity(new String(Arrays.copyOfRange(body, 172, 300)));
				this.startTime=getStartTime(body);
				
				//��������
				if(getColumns() == null || getColumns().length<=0) {
					setColumns(new String[]{"cdr"});
				}
				
				//������ֵ
				setValues(new String[]{line});
				
				//���
				HbaseHelper.addData(tab, getRowkey(linenum),  getFamilyNames()[0], getColumns(), getValues(), null);

			}
			tab.flushCommits();
//			if( tab!=table ) tab.close();
			if( null != br) br.close();
			LOG.info("end buildRecord");
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			LOG.error("MasterNotRunningException:" + e.getMessage());
			//System.out.println("MasterNotRunningException:" + e.getMessage());
			throw new InterruptedException();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			LOG.error("ZooKeeperConnectionException:" + e.getMessage());
			throw new InterruptedException();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			LOG.error("UnsupportedEncodingException:" + e.getMessage());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error("IOException:" + e.getMessage());
		}

		return linenum - 1;
	}

	public String getRowkey(int linenum) {
		StringBuilder hsb = new StringBuilder();
		hsb.append(getMobNo()).append("|").
		append(getStartTime()).append("|").
		append(get_fileName()).append("|").
		append(getSeqString(linenum));
		return hsb.toString();
	}
	public String getSeqString(int seq) {
		String seqString = Integer.toString(seq);
		StringBuilder ret = new StringBuilder();
		int shift = MAX_SEQ_LENGTH - seqString.length();
		for (int i = 0; i < shift; ++i) {
			ret.append("0");
		}
		ret.append(seqString);
		return ret.toString();
	}

	public String getStartTime(byte[] bytes) throws NumberFormatException, UnsupportedEncodingException {
		String starttime = null;
		int cdr_type = Integer.parseInt(new String(Arrays.copyOfRange(bytes, 10, 12), "GBK"));
		if (1 == cdr_type) {
			starttime = new String(Arrays.copyOfRange(bytes, 1402, 1417), "GBK");
		} else {
			starttime = new String(Arrays.copyOfRange(bytes, 1432, 1447), "GBK");
		}
		return starttime.trim();
	}

	public String getIdentity(String cdr) {
		cdr = cdr.trim();
		if (cdr.length() > 3) {
			if (cdr.charAt(0) == '+') {
				cdr = cdr.substring(1);
			}
			if ("86".equals(cdr.subSequence(0, 2))) {
				cdr = cdr.substring(2);
			}
		}

		return cdr;
	}


	public String get_fileName() {
		return _fileName;
	}


	public void set_fileName(String _fileName) {
		this._fileName = _fileName;
	}
	
	public String getCdr() {
		return cdr;
	}
	public void setCdr(String cdr) {
		this.cdr = cdr;
	}
	public String getMobNo() {
		return mobNo;
	}
	public void setMobNo(String mobNo) {
		this.mobNo = mobNo;
	}
	public String getStartTime() {
		return startTime;
	}
	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	

}
