package com.nexr.rhive.hive;
import hep.io.xdr.XDROutputStream;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;


public class RSerializer {
	private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
	
	private static int CACHED_MASK = (1 << 5);
	private static int HASHASH_MASK = 1;
	private static int IS_OBJECT_BIT_MASK = (1 << 8);
	private static int HAS_ATTR_BIT_MASK = (1 << 9);
	private static int HAS_TAG_BIT_MASK = (1 << 10);
	
	private static int LATIN1_BIT_MASK = (1 << 2);
	private static int ASCII_BIT_MASK = (1 << 6);
	private static int UDF8_BIT_MASK = (1 << 3);
	
	private static int LCK_BIT_MASK = (1 << 14);
	
	static enum Type {
		NIL(0),
		SYM(1),
		LIST(2),
		CHAR(9),
		LGC(10),
		INT(13),
		REAL(14),
		CPLX(15),
		STR(16),
		VEC(19);
		
		private int value;
		
		Type(int value) {
			this.value = value;
		}
		
		int getValue() {
			return value;
		}
	}
	
	private static final int NA_INTEGER = -2147483648;
	private static final int NA_REAL_HW = 0x7ff80000;
	private static final int NA_REAL_LW = 0x000007a2;
	
	
	private static int versionNumber(int v, int p, int s) {
		return ((v * 65536) + (p * 256) + s);
	}
	
	private static int flags(Type type, int levels, boolean isObject, boolean hasAttr, boolean hasTag) {
		int flags;
		if (type == Type.CHAR) {
			// scalar string type, used for symbol names
			levels &= (~(CACHED_MASK | HASHASH_MASK));
		}

		flags = type.getValue() | (levels << 12);
		if (isObject) {
			flags |= IS_OBJECT_BIT_MASK;
		}
		if (hasAttr) {
			flags |= HAS_ATTR_BIT_MASK;
		}
		if (hasTag) {
			flags |= HAS_TAG_BIT_MASK;
		}

		return flags;
	}
	
	public static void serialize(String name, File file, ResultSet rs) throws SQLException, IOException {
		ResultSetMetaData metaData = rs.getMetaData();
		int columnCount = metaData.getColumnCount();
		int[] columnTypes = new int[columnCount];
		String[] columnNames = new String[columnCount];
		
		List[] dataFrame = new List[columnCount];
		for (int i = 0; i < columnCount; i++) {
			columnNames[i] = metaData.getColumnName(i+1);

			int columnType = metaData.getColumnType(i+1);
			columnTypes[i] = columnType;
			dataFrame[i] = columnList(columnType);
		}
		
		int length = 0;
		while (rs.next()) {
			addColumns(dataFrame, columnTypes, rs);
			length++;
		}
		
		XDROutputStream os = null;
		try {
			os = new XDROutputStream(new BufferedOutputStream(new FileOutputStream(file), 64*1024));
			String magic = "RDX2";
			os.writeBytes(String.format("%s\n", magic));
			
			String format = "X";
			os.writeBytes(String.format("%s\n", format));
			
			writeCompatibilities(os);
			
			os.writeInt(flags(Type.LIST, 0x0, false, false, true));
			os.writeInt(flags(Type.SYM, 0x0, false, false, false));
			os.writeInt(flags(Type.CHAR, ASCII_BIT_MASK | CACHED_MASK | HASHASH_MASK, false, false, false));
			
			os.writeInt(name.length());
			os.writeBytes(name);
	
			writeData(os, dataFrame, columnTypes);
			writeAttributes(os, columnNames, length);
			
			os.writeInt(254);
			os.writeInt(254);
			
			os.flush();
		} finally {
			if (os != null) {
				os.close();
			}
		}
	}

	private static void writeCompatibilities(XDROutputStream os) throws IOException {
		int serializationVersion = 2;
		os.writeInt(serializationVersion);
		
		int version = versionNumber(2, 15, 2);
		os.writeInt(version);
		
		int minimalVersion = versionNumber(2, 3, 0);
		os.writeInt(minimalVersion);
	}

	private static void writeData(XDROutputStream os, List[] dataFrame, int[] columnTypes) throws IOException {
		os.writeInt(flags(Type.VEC, 0x0, true, true, false));
		os.writeInt(columnTypes.length);
		
		for (int i = 0; i < columnTypes.length; i++) {
			int length = dataFrame[i].size();

			switch(columnTypes[i]) {
			case Types.VARCHAR:
				os.writeInt(flags(Type.STR, 0x0, false, false, false));
				os.writeInt(length);
				
				for (int j = 0; j < length; j++) {
					String value = (String) dataFrame[i].get(j);
					
					os.writeInt(flags(Type.CHAR, CACHED_MASK, false, false, false));
					if (value == null || value.length() == 0) {
						os.writeInt(-1);
					} else {
						os.writeInt(value.length());
						os.writeBytes(value);
					}
				}

				break;
			case Types.BOOLEAN:
				os.writeInt(flags(Type.LGC, 0x0, false, false, false));
				os.writeInt(length);
				
				for (int j = 0; j < length; j++) {
					Boolean value = (Boolean) dataFrame[i].get(j);
					if (value == null) {
						os.writeInt(NA_INTEGER);
					} else {
						os.writeInt(value == true ? 1 : 0);
					}
				}
				
				break;
			case Types.TINYINT:
			case Types.SMALLINT:
			case Types.INTEGER:
				os.writeInt(flags(Type.INT, 0x0, false, false, false));
				os.writeInt(length);
				
				for (int j = 0; j < length; j++) {
					Integer value = (Integer) dataFrame[i].get(j);
					if (value == null) {
						os.writeInt(NA_INTEGER);
					} else {
						os.writeInt(value);
					}
				}
	
				break;
			case Types.FLOAT:
				os.writeInt(flags(Type.REAL, 0x0, false, false, false));
				os.writeInt(length);
				
				for (int j = 0; j < length; j++) {
					Float value = (Float) dataFrame[i].get(j);
					if (value == null) {
						os.writeInt(NA_REAL_HW);
						os.writeInt(NA_REAL_LW);
					} else {
						os.writeDouble(value.doubleValue());
					}
				}
				
				break;
			case Types.DOUBLE:
				os.writeInt(flags(Type.REAL, 0x0, false, false, false));
				os.writeInt(length);
				
				for (int j = 0; j < length; j++) {
					Double value = (Double) dataFrame[i].get(j);
					if (value == null) {
						os.writeInt(NA_REAL_HW);
						os.writeInt(NA_REAL_LW);
					} else {
						os.writeDouble(value);
					}
				}
				
				break;
			case Types.BIGINT:
				os.writeInt(flags(Type.REAL, 0x0, false, false, false));
				os.writeInt(length);
				
				for (int j = 0; j < length; j++) {
					Long value = (Long) dataFrame[i].get(j);
					if (value == null) {
						os.writeInt(NA_REAL_HW);
						os.writeInt(NA_REAL_LW);
					} else {
						os.writeDouble(value.doubleValue());
					}
				}
				
				break;
			case Types.TIMESTAMP:
			default:
				os.writeInt(flags(Type.STR, 0x0, false, false, false));
				os.writeInt(length);
				
				for (int j = 0; j < length; j++) {
					String value = (String) dataFrame[i].get(j);

					os.writeInt(flags(Type.CHAR, CACHED_MASK, false, false, false));
					if (value == null || value.length() == 0) {
						os.writeInt(-1);
					} else {
						os.writeInt(value.length());
						os.writeBytes(value);
					}
				}
			}
		}
	}

	private static void writeAttributes(XDROutputStream os, String[] columnNames, int length)
			throws IOException {
		os.writeInt(flags(Type.LIST, 0x0, false, false, true));
		os.writeInt(flags(Type.SYM, 0x0, false, false, false));
		os.writeInt(flags(Type.CHAR, ASCII_BIT_MASK | CACHED_MASK | HASHASH_MASK, false, false, false));
		os.writeInt(5);
		os.writeBytes("names");

		os.writeInt(flags(Type.STR, 0x0, false, false, false));
		os.writeInt(columnNames.length);

		for (int i = 0; i < columnNames.length; i++) {
			os.writeInt(flags(Type.CHAR, ASCII_BIT_MASK | CACHED_MASK | HASHASH_MASK, false, false, false));
			os.writeInt(columnNames[i].length());
			os.writeBytes(columnNames[i]);
		}
		
		os.writeInt(flags(Type.LIST, 0x0, false, false, true));
		os.writeInt(flags(Type.SYM, 0x0, false, false, false));
		os.writeInt(flags(Type.CHAR, ASCII_BIT_MASK | CACHED_MASK | HASHASH_MASK, false, false, false));
		os.writeInt(9);
		os.writeBytes("row.names");
		
		os.writeInt(flags(Type.INT, 0x0, false, false, false));
		os.writeInt(2);
		os.writeInt(NA_INTEGER);
		os.writeInt(-1 * length);
		
		os.writeInt(flags(Type.LIST, 0x0, false, false, true));
		os.writeInt(flags(Type.SYM, 0x0, false, false, false));
		os.writeInt(flags(Type.CHAR, ASCII_BIT_MASK | CACHED_MASK | HASHASH_MASK, false, false, false));
		os.writeInt(5);
		os.writeBytes("class");

		os.writeInt(flags(Type.STR, 0x0, false, false, false));
		os.writeInt(1);
		os.writeInt(flags(Type.CHAR, ASCII_BIT_MASK | CACHED_MASK | HASHASH_MASK, false, false, false));
		os.writeInt(10);
		os.writeBytes("data.frame");
	}
	
	private static List columnList(int columnType) {
		switch(columnType) {
		case Types.VARCHAR:
			return new ArrayList<String>();
		case Types.BOOLEAN:
			return new ArrayList<Boolean>();
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
			return new ArrayList<Integer>();
		case Types.FLOAT:
			return new ArrayList<Float>();
		case Types.DOUBLE:
			return new ArrayList<Double>();
		case Types.BIGINT:
			return new ArrayList<Long>();
		case Types.TIMESTAMP:
			return new ArrayList<String>();
		default:
			return new ArrayList<String>();
		}
	}
	
	private static void addColumns(List[] dataFrame, int[] columnTypes, ResultSet rs) throws SQLException {
		for (int i = 1; i <= columnTypes.length; i++) {
			switch(columnTypes[i-1]) {
			case Types.VARCHAR:
				dataFrame[i-1].add(rs.getString(i));
				break;
			case Types.BOOLEAN:
				dataFrame[i-1].add(rs.getBoolean(i));
				break;
			case Types.TINYINT:
			case Types.SMALLINT:
			case Types.INTEGER:
				dataFrame[i-1].add(rs.getInt(i));
				break;
			case Types.FLOAT:
				dataFrame[i-1].add(rs.getFloat(i));
				break;
			case Types.DOUBLE:
				dataFrame[i-1].add(rs.getDouble(i));
				break;
			case Types.BIGINT:
				dataFrame[i-1].add(rs.getLong(i));
				break;
			case Types.TIMESTAMP:
				String ts = dateFormat.format(rs.getTimestamp(i));
				dataFrame[i-1].add(ts);
				break;
			default:
				dataFrame[i-1].add(rs.getString(i));
			}
		}
	}
}
