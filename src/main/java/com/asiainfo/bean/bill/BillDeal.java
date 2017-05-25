package com.asiainfo.bean.bill;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import com.asiainfo.base.HbaseHelper;
import com.asiainfo.bean.Base;

public class BillDeal extends Base {
	public static final String START = "START|";
	public static final String END = "END";
	
	public static int ACCNO_INDEX = 0;
	public static int USERID_INDEX = 1;
	public static int MOBNO_INDEX = 2;
	public static int DATE_INDEX = 4;
	public static int VERSION_INDEX = -5;
	public static int TYPE_INDEX = -4;
	public static int OLD_BILL_LENGTH = 20;
	
	public static final char DELIMITER = '|';
	public static final char BODY_ITEM_DELIMITER = '^';
	
	private boolean _isGroupBill = false;
	private boolean _isOldBill = false;
	
	private String _mobNo;
	private String _accNo;
	private String _userId;
	private String _yearMonth;
	private String _version;
	private String _type;
	private String _area;
	
	@Override
	public boolean checkFileName(String name) {
		/**
		 * CXBILL��HWBILL��CXBILLNEW��HWBILLNEW��CXFLOWBILL��HWFLOWBILL
		 */
		boolean flag=false;
		if(name.toUpperCase().startsWith("CXBILL_") || name.toUpperCase().startsWith("HWBILL_") ||
		   name.toUpperCase().startsWith("CXBILLNEW_") || name.toUpperCase().startsWith("HWBILLNEW_") ||
		   name.toUpperCase().startsWith("CXFLOWBILL_") || name.toUpperCase().startsWith("HWFLOWBILL_")){
			flag=true;
		}
		return flag;
	}


	@Override
	public int buildRecord(HTable table,
			   String filename,
			   BufferedReader br) throws Exception {
		int billcount = 0;
		String line = "";
        boolean bflag = false;  //��ʶ�˵������˵��Ƿ��д�������д��������˵�������װ��ֱ����װ��һ��
        StringBuilder body = new StringBuilder();
        String head = "";
        
        getFileType(filename);
        //���ļ��������ڽ���
        
        String tmptname = null;
		String tanameame = filename.split("_")[2].substring(0, 6);
		String taname = new String(table.getTableName());
		if(taname.endsWith(tanameame)) {
			//���Ѿ����ڣ�����Ҫ���´���
			tmptname = taname.substring(0, taname.length() - 6) + tanameame;
		}else{
			System.out.println("currFileMonth:" + tanameame);
			if ( mapTable.containsKey(tanameame) == false ){
				System.out.println("��ʼ������: "+taname.substring(0, taname.length() - 6) + tanameame);
				if(getRegions().length > 1 || !"".equals(getRegions()[0])){
					byte[][] regs = new byte[getRegions().length][];
					for(int j = 0;j < getRegions().length; j++){
						regs[j] = Bytes.toBytes(getRegions()[j]);
					}
					HbaseHelper.creatTable(taname.substring(0, taname.length() - 6) + tanameame, getFamilyNames(), regs);
				}else{
					HbaseHelper.creatTable(taname.substring(0, taname.length() - 6) + tanameame, getFamilyNames(), null);
				}
				table = new HTable(HbaseHelper.conf, Bytes.toBytes(taname.substring(0, taname.length() - 6) + tanameame));
				table.setAutoFlush(false);
				table.flushCommits();
				mapTable.put(tanameame, table);
			}else {
				table = mapTable.get(tanameame);
			}
			tmptname = taname.substring(0, taname.length() - 6) + tanameame;
		}
		//���ñ���
		setTabName(tmptname);
	
		
		while (((line = br.readLine()) != null)) {
			//�˵�ͷ����
			if (line.startsWith(START)) {
				head = line.substring(START.length(), line.length());
				//�����˵�ͷ
				parseHeader(head);
//				body.delete(0, body.length());  //���֮ǰbody����
				body.setLength(0); //���֮ǰbody����  Ч����΢�õ�
				bflag = false;
				continue;
    		}
    		
    		//�˵������
			if ((!line.matches(".*\\|.*\\|.*") || line.split("\\|",-1).length != 3) && 
				 !line.startsWith(END)) {
				System.out.println("error mobile:"+get_mobNo()+"error context:"+line);
				bflag = true;
			}
			
			if(bflag) {
				continue; //����ƴ���˵���
			} else {
				String reDelimitedLine = line.replace(DELIMITER, BODY_ITEM_DELIMITER);
				if(!line.startsWith(END))
					body.append(reDelimitedLine).append("|");
			}
			
			//�˵�β��;������
    		if(line.equals(END)) {
    			billcount++;
    			
    			//��������
/*				if(getColumns() == null || getColumns().length<=0) {
					setColumns(new String[]{"Header","Area","Body"});
				}*/
				
				//������ֵ
				setValues(new String[]{head,_area,body.toString()});
				
				//���
				HbaseHelper.addData(table,
						getHBaseRowKey(),
						getFamilyNames()[0],
						getColumns(),
						getValues(),
						"GBK");
				
    		}
		}
		table.flushCommits();
		br.close();
		return billcount;
	}

	/**
	 * ��ȡrowkey
	 * @return
	 */
	public String getHBaseRowKey() {
		StringBuilder sb = new StringBuilder();
		if (this._isGroupBill) {
			sb.append(get_accNo());
			sb.append('|');
			sb.append(get_mobNo());
		} else {
			sb.append(get_mobNo());
			sb.append('|');
			sb.append(get_accNo());
			sb.append('|');
			sb.append(get_yearMonth());
			sb.append('|');
			sb.append(get_area());
			sb.append('|');
			if (this._isOldBill) {
				sb.append('|');
			} else {
				sb.append(get_version());
				sb.append('|');
				sb.append(get_type());
			}
		}
		return sb.toString();
	}
	
	/**
	 * ���˵�ͷ��ȡ���ѶϢ
	 * @param header
	 */
	private void parseHeader(String header) {
		List<String> split = split(header, '|');

		this._accNo = promisedGet(split, ACCNO_INDEX);
		this._userId = promisedGet(split, USERID_INDEX);
		this._mobNo = promisedGet(split, MOBNO_INDEX);
		this._yearMonth = promisedGet(split, DATE_INDEX);
		if (this._isOldBill) {
			this._version = "";
			this._type = "";
		} else {
			this._version = promisedGet(split, VERSION_INDEX);
			this._type = promisedGet(split, TYPE_INDEX);
		}
	}
	
	/**
	 * ��ȡ������Ԫ��
	 * @param split Դ����
	 * @param index ����λ��
	 * @return
	 */
	private String promisedGet(List<String> split, int index) {
		if (index >= split.size()) {
			return "";
		}
		if (index < 0) {
			return (String) split.get(split.size() + index);
		}
		return (String) split.get(index);
	}
	
	/**
	 * �����ļ�����ȡ�ļ�����
	 * @param fileName
	 * @throws IOException
	 */
	public void getFileType(String fileName)
			throws IOException {
		if (fileName.startsWith("JT")) {
			this._isGroupBill = true;
			this._area = "";
		} else {
			this._isGroupBill = false;
			if ((fileName.contains("NEW")) || (fileName.contains("FLOW"))) {
				this._isOldBill = false;
			} else {
				this._isOldBill = true;
			}
			this._area = fileName.split("_")[1];
		}
	}
	
	private List<String> split(String line, char del) {
		List<String> ret = new ArrayList<String>();
		int start = 0;
		while (start < line.length()) {
			int end = line.indexOf(del, start);
			//���indexOfȡֵ������󳤶Ƚ��᷵��-1 ,end����ֵ����󳤶�
			if (end == -1) {
				end = line.length();
			}
			ret.add(line.substring(start, end));
			start = end + 1;
		}
		return ret;
	}

	public String get_mobNo() {
		return _mobNo;
	}

	public void set_mobNo(String _mobNo) {
		this._mobNo = _mobNo;
	}

	public String get_accNo() {
		return _accNo;
	}

	public void set_accNo(String _accNo) {
		this._accNo = _accNo;
	}

	public String get_userId() {
		return _userId;
	}

	public void set_userId(String _userId) {
		this._userId = _userId;
	}

	public String get_yearMonth() {
		return _yearMonth;
	}

	public void set_yearMonth(String _yearMonth) {
		this._yearMonth = _yearMonth;
	}

	public String get_version() {
		return _version;
	}

	public void set_version(String _version) {
		this._version = _version;
	}

	public String get_type() {
		return _type;
	}

	public void set_type(String _type) {
		this._type = _type;
	}

	public String get_area() {
		return _area;
	}

	public void set_area(String _area) {
		this._area = _area;
	}
	
	

}
