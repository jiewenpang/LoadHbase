package com.asiainfo.tool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

public class SSHClient {
	private String ip;
	private String username;
	private String password;
	private int port;
	private Connection conn = null;
	
//	static Logger log = Logger.getLogger(SSHClient.class);

	
	private SSHClient(String ip, int port, String username, String password){
		this.ip = ip;
		this.port = port;
		this.username = username;
		this.password = password;
		
	}
	
	public static SSHClient NewInstance(String ip, int port, String username, String password){
		return new SSHClient(ip, port, username, password);
	}

	
	public List<String> getFileList(String cmd) {
		List<String> flist = null;
		try
		{
			if(null == conn){
				throw new ConnectException();
			}

			/* Create a session */

			Session sess = conn.openSession();
//			System.out.println("cmd:"+cmd);
			
			sess.execCommand(cmd);
			
//			System.out.println("Here is some information about the remote host:");
			/* 
			 * This basic example does not handle stderr, which is sometimes dangerous
			 * (please read the FAQ).
			 */

			InputStream stdout = new StreamGobbler(sess.getStdout());

			BufferedReader br = new BufferedReader(new InputStreamReader(stdout));
			flist = new ArrayList<String>();
			String line = br.readLine();
			while(null != line){
				flist.add(line);
				line = br.readLine();
			}

			/* Show exit status, if available (otherwise "null") */

			/* Close this InputStream */
			if(null != stdout){
				stdout.close();
			}
			/* Close this BufferedReader */
			if(null != br){
				br.close();
			}
			/* Close this session */
			if(null != sess){
				sess.close();
			}
			/* Close the connection */
			if(null != conn){
				conn.close();
			}

		}
		catch (IOException e)
		{
			e.printStackTrace(System.err);
			return flist;
		}
		
		return flist;
	}
	
	public  boolean getConnection() throws IOException {
		
		conn = new Connection(ip, port);

		/* Now connect */
		conn.connect();

		/* Authenticate.
		 * If you get an IOException saying something like
		 * "Authentication method password not supported by the server at this stage."
		 * then please check the FAQ.
		 */
		
		/**/boolean isAuthenticated = conn.authenticateWithPassword(username, password);
		
		if (isAuthenticated == false)
			throw new IOException("Authentication failed.");
		
		return isAuthenticated;
	}
	
}
