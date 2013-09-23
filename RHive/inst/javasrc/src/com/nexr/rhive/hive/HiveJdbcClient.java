package com.nexr.rhive.hive;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.StringTokenizer;
 
public class HiveJdbcClient implements HiveOperations {
	private DatabaseConnection databaseConnection;
	private int version;
	private ResultSet columns;
	

	public HiveJdbcClient(boolean hiveServer2) {
		if (hiveServer2) {
			this.version = 2;
		} else {
			this.version = 1;
		}
		
	}
	
	public void connect(String host, int port) throws SQLException {
		connect(host, port, "default", null, null);
	}

	public void connect(String host, int port, String db) throws SQLException {
		connect(host, port, db, null, null);
	}

	public void connect(String host, int port, String user, String password) throws SQLException {
		connect(host, port, "default", user, password);
	}
	
	public void connect(String host, int port, String db, String user, String password) throws SQLException {
		HiveJdbcConnector hiveConnector = new HiveJdbcConnector(host, port, db, user, password);
		hiveConnector.setDaemon(false);
		hiveConnector.start();
		try {
			hiveConnector.join(5000);
		} catch (InterruptedException e) { throw new SQLException(e); }

		if (hiveConnector.isAlive()) {
			int version = getVersion();
			throw new SQLException(
					String.format(
							"Connection to hiveserver has timed out.\n" +
							"\t--connection-parameters(version: hiveserver%d, host:%s, port:%d, db:%s, user:%s, password:%s)\n\n" + 
							"Restart R session and retry with correct arguments.",
							 version, host, port, db, user, password));
		}
	}

	public void close() throws SQLException {
		if (databaseConnection != null) {
			databaseConnection.close();
		}
	}
	
	DatabaseConnection getDatabaseConnection() {
		return databaseConnection;
	}

	
	public Connection getConnection(boolean reconnect) throws SQLException {
		checkConnection();
		DatabaseConnection connection = getDatabaseConnection();
		return connection.getConnection(reconnect);
	}

	DatabaseMetaData getDatabaseMetaData() throws SQLException {
		checkConnection();
		DatabaseConnection connection = getDatabaseConnection();
		if (connection.getDatabaseMetaData() == null) {
			throw new IllegalStateException("Not connected to hiveserver");
		}
		return connection.getDatabaseMetaData();
	}
	
	public void checkConnection() throws SQLException {
		if (getDatabaseConnection() == null) {
			throw new IllegalStateException("Not connected to hiveserver");
		}
	}
	
	/*
	QueryResult getColumns(String table) throws SQLException {
		DatabaseMetaData databaseMetaData = getDatabaseConnection().getDatabaseMetaData();
		ResultSet rs = databaseMetaData.getColumns(databaseMetaData.getConnection().getCatalog(), null, table, "%");
		return new QueryResult(rs);
	}
	
	QueryResult getTables() throws SQLException {
		DatabaseMetaData databaseMetaData = getDatabaseConnection().getDatabaseMetaData();
		ResultSet rs = databaseMetaData.getTables(databaseMetaData.getConnection().getCatalog(), null, "%", new String[] { "TABLE" });
		return new QueryResult(rs);
	}
	*/

	String dequote(String str) {
		if (str == null) {
			return null;
		}
		while ((str.startsWith("'") && str.endsWith("'"))
				|| (str.startsWith("\"") && str.endsWith("\""))) {
			str = str.substring(1, str.length() - 1);
		}
		return str;
	}

	String[] split(String line, String delim) {
		StringTokenizer tok = new StringTokenizer(line, delim);
		String[] ret = new String[tok.countTokens()];
		int index = 0;
		while (tok.hasMoreTokens()) {
			String t = tok.nextToken();
			t = dequote(t);
			ret[index++] = t;
		}
		return ret;
	}

	static String replace(String source, String from, String to) {
		if (source == null) {
			return null;
		}

		if (from.equals(to)) {
			return source;
		}

		StringBuilder replaced = new StringBuilder();

		int index = -1;
		while ((index = source.indexOf(from)) != -1) {
			replaced.append(source.substring(0, index));
			replaced.append(to);
			source = source.substring(index + from.length());
		}
		replaced.append(source);

		return replaced.toString();
	}

	@Override
	public boolean execute(String query) throws SQLException {
		return execute(query, false);
	}

	protected boolean execute(String query, boolean reconnect) throws SQLException {
		Connection connection = getConnection(reconnect);
		Statement statement = null;
		try {
			statement = connection.createStatement();
			return statement.execute(query);
		} catch (SQLException e) {
			if (!reconnect) {
				if (isThriftTransportException(e)) {
					return reexecute(query);
				}
			}
			
			throw e;
		}
	}

	protected boolean reexecute(String query) throws SQLException {
		return execute(query, true);
	}

	@Override
	public QueryResult query(String query) throws SQLException {
		return query(query, 0, 50);
	}

	@Override
	public QueryResult query(String query, int fetchSize) throws SQLException {
		return query(query, 0, fetchSize);
	}

	@Override
	public QueryResult query(String query, int maxRows, int fetchSize) throws SQLException {
		return query(query, maxRows, fetchSize, false);
	}
	
	protected QueryResult query(String query, int maxRows, int fetchSize, boolean reconnect) throws SQLException {
		Connection connection = getConnection(reconnect);
		Statement statement = null;
		try {
			statement = connection.createStatement();
			statement.setMaxRows(maxRows < 0 ? 0 : maxRows);
			statement.setFetchSize(fetchSize);
			ResultSet rs = statement.executeQuery(query);
			
			return new QueryResult(rs);
		} catch (SQLException e) {
			if (!reconnect) {
				if (isThriftTransportException(e)) {
					return requery(query, maxRows, fetchSize);
				}
			}
			
			throw e;
		}
	}
	
	protected QueryResult requery(String query, int maxRows, int fetchSize) throws SQLException {
		return query(query, maxRows, fetchSize, true);
	}

	@Override
	public boolean set(String key, String value) throws SQLException {
		String command = String.format("SET %s=%s", key, value);
		execute(command);
		return true;
	}
	
	@Override
	public QueryResult listConfigs(boolean all) throws SQLException {
		String command = all ? "SET -v" : "SET";
		return query(command);
	}
	
	@Override
	public boolean addJar(String jar) throws SQLException {
		if (jar == null) {
			return false;
		}
		
		String command = String.format("ADD JAR %s", jar);
		execute(command);
		return true;
	}
	
	@Override
	public boolean addFile(String file) throws SQLException {
		if (file == null) {
			return false;
		}
		
		String command = String.format("ADD FILE %s", file);
		execute(command);
		return true;
	}

	@Override
	public boolean addArchive(String archive) throws SQLException {
		if (archive == null) {
			return false;
		}
		
		String command = String.format("ADD ARCHIVE %s", archive);
		execute(command);
		return true;
	}
	
	@Override
	public boolean deleteJar(String jar) throws SQLException {
		if (jar == null) {
			return false;
		}
		
		String command = String.format("DELETE JAR %s", jar);
		execute(command);
		return true;
	}

	@Override
	public boolean deleteFile(String file) throws SQLException {
		if (file == null) {
			return false;
		}
		
		String command = String.format("DELETE FILE %s", file);
		execute(command);
		return true;
	}

	@Override
	public boolean deleteArchive(String archive) throws SQLException {
		if (archive == null) {
			return false;
		}
		
		String command = String.format("DELETE ARCHIVES %s", archive);
		execute(command);
		return true;

	}
	
	@Override
	public int getVersion() {
		return version;
	}

	protected boolean isThriftTransportException(Exception e) {
		String msg = e.getMessage();
		return msg.indexOf("TTransportException") != -1;
	}
	
	
	private class HiveJdbcConnector extends Thread {
		
		private String host;
		private int port;
		private String db;
		private String user;
		private String password;

		public HiveJdbcConnector(String host, int port, String db, String user, String password) {
			this.host = host;
			this.port = port;
			this.db = db;
			this.user = user;
			this.password = password;
		}
		
		@Override
		public void run() {
			connect(host, port, db, user, password);
		}
		
		public void connect(String host, int port, String db, String user, String password) {
			try {
				String url = getUrl(host, port, db);
				String driver = getDriver();
				DatabaseConnection connection = new DatabaseConnection(driver, url, user, password);
				connection.connect();
				
				databaseConnection = connection;
			} catch (Exception e) {
				if (e instanceof RuntimeException) {
					throw (RuntimeException) e;
				} else {
					throw new RuntimeException(e);
				}
			}
		}
		
		private String getUrl(String host, int port, String db) {
			String scheme = getUrlPrefix();
			StringBuilder sb = new StringBuilder(scheme);
			sb.append(host);
			sb.append(":");
			sb.append(port);
			sb.append("/");
			sb.append(db);

			return sb.toString();
		}

		
		private String getUrlPrefix() {
			switch (version) {
			case 1:
				return "jdbc:hive://";
			case 2:
				return "jdbc:hive2://";
			default:
				return null;
			}
		}
		
		private String getDriver() {
			switch(version) {
			case 1:
				return "org.apache.hadoop.hive.jdbc.HiveDriver";
			case 2:
				return "org.apache.hive.jdbc.HiveDriver";
			default:
				return null;
			}
		}
	}
}