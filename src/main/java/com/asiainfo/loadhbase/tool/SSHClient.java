package com.asiainfo.loadhbase.tool;

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

	private SSHClient(String ip, int port, String username, String password) {
		this.ip = ip;
		this.port = port;
		this.username = username;
		this.password = password;

	}

	public static SSHClient NewInstance(String ip, int port, String username, String password) {
		return new SSHClient(ip, port, username, password);
	}

	public List<String> getFileList(String cmd) {
		List<String> flist = null;
		
		try {
			if (null == conn) {
				throw new ConnectException();
			}

			Session sess = conn.openSession();

			sess.execCommand(cmd);
			
			InputStream stdout = new StreamGobbler(sess.getStdout());

			BufferedReader br = new BufferedReader(new InputStreamReader(stdout));
			flist = new ArrayList<String>();
			String line = br.readLine();
			while (null != line) {
				flist.add(line);
				line = br.readLine();
			}

			/* Close this InputStream */
			if (null != stdout) {
				stdout.close();
			}
			/* Close this BufferedReader */
			if (null != br) {
				br.close();
			}
			/* Close this session */
			if (null != sess) {
				sess.close();
			}
			/* Close the connection */
			if (null != conn) {
				conn.close();
			}

		} catch (IOException e) {
			e.printStackTrace(System.err);
			return flist;
		}

		return flist;
	}

	public boolean getConnection() throws IOException {
		conn = new Connection(ip, port);
		conn.connect();

		boolean isAuthenticated = conn.authenticateWithPassword(username, password);

		if (isAuthenticated == false)
			throw new IOException("Authentication failed.");

		return isAuthenticated;
	}

}
