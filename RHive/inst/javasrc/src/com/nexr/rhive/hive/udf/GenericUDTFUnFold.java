/**
 * Copyright 2011 NexR
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.nexr.rhive.hive.udf;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.nexr.rhive.hive.HiveVariations;

/**
 * UnFoldUDTF
 * 
 */
@Description(name = "unfold", value = "_FUNC_(value,arg1,arg2,...,delim) - Returns the extended result of one column")
public class GenericUDTFUnFold extends GenericUDTF {

	enum DATA_TYPE {
		STRING, DOUBLE, INT, NUMERIC, NULLNAME
	};

	int numCols; // number of output columns

	DATA_TYPE[] data_types; // mapping from data-types to enum DATA_TYPE
	Writable[] retCols; // array of returned column values
	Writable[] cols; // object pool of non-null writable, avoid creating
						// objects all the time
	Object[] nullCols; // array of null column values
	ObjectInspector[] inputOIs; // input ObjectInspectors
	boolean pathParsed = false;
	boolean seenErrors = false;
	String delim = ",";

	@Override
	public StructObjectInspector initialize(ObjectInspector[] args)
			throws UDFArgumentException {
		inputOIs = args;
		numCols = args.length - 2;

		if (args.length < 3) {
			throw new UDFArgumentException(
					"unfold() takes at least three arguments: "
							+ "the origin string, data type and deliminator");
		}

		for (int i = 0; i < args.length; ++i) {
			if (args[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
				throw new UDFArgumentException(
						"unfold()'s arguments have to be primitive type");
			}
		}

		seenErrors = false;
		pathParsed = false;
		// paths = new String[numCols];
		data_types = new DATA_TYPE[numCols];
		cols = new Writable[numCols];
		retCols = new Writable[numCols];
		nullCols = new Object[numCols];

		// construct output object inspector
		ArrayList<String> fieldNames = new ArrayList<String>(numCols);
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(
				numCols);
		for (int i = 0; i < numCols; ++i) {
			// column name can be anything since it will be named by UDTF as
			// clause
			fieldNames.add("c" + i);
			// all returned type will be primitive type
			try {
				fieldOIs.add(getColumnInspector(args[i + 1].getTypeName()));
				cols[i] = getColumnWritable(args[i + 1].getTypeName());
			} catch (Exception e) {
				throw new UDFArgumentException(e);
			}
			retCols[i] = cols[i];
			nullCols[i] = null;
		}

		return ObjectInspectorFactory.getStandardStructObjectInspector(
				fieldNames, fieldOIs);
	}

	private ObjectInspector getColumnInspector(String typeName)
			throws IllegalArgumentException, SecurityException, IllegalAccessException, NoSuchFieldException {
		if (typeName.equals(HiveVariations.getFieldValue(HiveVariations.serdeConstants, "INT_TYPE_NAME"))) {
			return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
		} else if (typeName.equals(HiveVariations.getFieldValue(HiveVariations.serdeConstants, "DOUBLE_TYPE_NAME"))) {
			return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
		} else if (typeName.equals(HiveVariations.getFieldValue(HiveVariations.serdeConstants, "STRING_TYPE_NAME"))) {
			return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
		} else
			throw new IllegalArgumentException("can't support this type " + typeName);
	}

	private Writable getColumnWritable(String typeName)
			throws IllegalArgumentException, SecurityException, IllegalAccessException, NoSuchFieldException {
		if (typeName.equals(HiveVariations.getFieldValue(HiveVariations.serdeConstants, "INT_TYPE_NAME"))) {
			return new IntWritable(0);
		} else if (typeName.equals(HiveVariations.getFieldValue(HiveVariations.serdeConstants, "DOUBLE_TYPE_NAME"))) {
			return new DoubleWritable(0.0);
		} else if (typeName.equals(HiveVariations.getFieldValue(HiveVariations.serdeConstants, "STRING_TYPE_NAME"))) {
			return new Text();
		} else
			throw new IllegalArgumentException("can't support this type : " + typeName);
	}

	@Override
	public void process(Object[] args) throws HiveException {
		if (args[0] == null) {
			forward(nullCols);
			return;
		}
		// get the path names for the 1st row only
		if (!pathParsed) {
			for (int i = 0; i < numCols; ++i) {
				if (inputOIs[i + 1] instanceof StringObjectInspector) {
					data_types[i] = DATA_TYPE.STRING;
				} else if (inputOIs[i + 1] instanceof DoubleObjectInspector) {
					data_types[i] = DATA_TYPE.DOUBLE;
				} else if (inputOIs[i + 1] instanceof IntObjectInspector) {
					data_types[i] = DATA_TYPE.INT;
				} else {
					data_types[i] = DATA_TYPE.NULLNAME;
				}
			}

			String local_delim = ((StringObjectInspector) inputOIs[args.length - 1])
					.getPrimitiveJavaObject(args[args.length - 1]);
			if (local_delim != null && !local_delim.equals(""))
				delim = local_delim;

			pathParsed = true;
		}

		String originStr = ((StringObjectInspector) inputOIs[0])
				.getPrimitiveJavaObject(args[0]);
		if (originStr == null) {
			forward(nullCols);
			return;
		}

		try {
			String[] originStrs = parseString(originStr, delim);

			if (originStrs.length != numCols) {

				StringBuffer sb = new StringBuffer();

				for (int i = 0; i < originStrs.length - 1; i++) {
					sb.append(originStrs[i] + delim);
				}
				sb.append(originStrs[originStrs.length - 1]);

				throw new HiveException("original data count["
						+ originStrs.length
						+ "] doesn't match unfold column count[ " + numCols
						+ "] : original data {" + sb.toString() + "}");

			}

			for (int i = 0; i < numCols; ++i) {

				if (originStrs[i] == null)
					retCols[i] = cols[i]; // use the object pool rather than
											// creating a new object

				switch (data_types[i]) {
				case STRING:
					((Text) retCols[i]).set(originStrs[i]);
					break;
				case DOUBLE:
					((DoubleWritable) retCols[i]).set(Double
							.parseDouble(originStrs[i]));
					break;
				case INT:
					((IntWritable) retCols[i]).set((int) Double
							.parseDouble(originStrs[i]));
					break;
				case NULLNAME:
				default:
					retCols[i] = cols[i];
				}
			}

			forward(retCols);
			return;
		} catch (Exception e) {
			if (!seenErrors) {
				seenErrors = true;
			}
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			e.printStackTrace(new PrintStream(output));
			throw new HiveException(new String(output.toByteArray()));

			// forward(nullCols);
			// return;
		}
	}

	public String toString() {
		return "unfold";
	}

	/**
	 * Called to notify the UDTF that there are no more rows to process. Clean
	 * up code or additional forward() calls can be made here.
	 */
	public void close() throws HiveException {
		// TODO Auto-generated method stub

	}

	private String[] parseString(String originStr, String delim) {
		StringTokenizer st = new StringTokenizer(originStr, delim);
		List<String> strs = new ArrayList<String>();

		while (st.hasMoreTokens()) {
			strs.add(st.nextToken());
		}

		return strs.toArray(new String[0]);
	}
}
