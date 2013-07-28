package com.nexr.rhive.hive;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class QueryResult {
	private ResultSet rs;
	
	public QueryResult(ResultSet rs) {
		this.rs = rs;
	}

	public String[] getNext() throws SQLException {		
		ResultSetMetaData metaData = rs.getMetaData();
		int columnCount = metaData.getColumnCount();
		
		String[] rec = new String[columnCount];
		if (rs.next()) {
			for (int i = 1; i <= columnCount; i++) {
				rec[i-1] = (rs.getString(i));
			}
			
			return rec;
		}
		
		return null;
	}
	
	public void close() throws SQLException {
		rs.close();
	}
	
	public String[] getColumns() throws SQLException {
		ResultSetMetaData metaData = rs.getMetaData();
		int columnCount = metaData.getColumnCount();
		
		List<String> columns = new ArrayList<String>(columnCount);
		for (int i = 1; i <= columnCount; i++) {
			columns.add(metaData.getColumnName(i));
		}
		
		return columns.toArray(new String[columns.size()]);
	}

	public String[] getColumnTypes() throws SQLException {
		ResultSetMetaData metaData = rs.getMetaData();
		int columnCount = metaData.getColumnCount();
		
		List<String> types = new ArrayList<String>(columnCount);
		for (int i = 1; i <= columnCount; i++) {
			types.add(metaData.getColumnTypeName(i));
		}
		
		return types.toArray(new String[types.size()]);
	}
}
