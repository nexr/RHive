package com.nexr.rhive.hive;

import java.sql.SQLException;



public interface HiveOperations {
	void connect(String host, int port) throws SQLException;
	void connect(String host, int port, String db) throws SQLException;
	void connect(String host, int port, String user, String password) throws SQLException;
	void connect(String host, int port, String db, String user, String password) throws SQLException;
	void close() throws SQLException;
	void checkConnection() throws SQLException;
	
	boolean execute(String query) throws SQLException;
	QueryResult query(String query) throws SQLException;
	QueryResult query(String query, int fetchSize) throws SQLException;
	QueryResult query(String query, int maxRows, int fetchSize) throws SQLException;

	boolean set(String key, String value) throws SQLException;
	QueryResult listConfigs(boolean all) throws SQLException;
	boolean addJar(String jar) throws SQLException;
	boolean addFile(String file) throws SQLException;
	boolean addArchive(String archive) throws SQLException;
	boolean deleteJar(String jar) throws SQLException;
	boolean deleteFile(String file) throws SQLException;
	boolean deleteArchive(String archive) throws SQLException;
	int getVersion();
}
