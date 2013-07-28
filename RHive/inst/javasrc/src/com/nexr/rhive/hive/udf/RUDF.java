/**
 * Copyright 2011 NexR
 *   
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nexr.rhive.hive.udf;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPDouble;
import org.rosuda.REngine.REXPInteger;
import org.rosuda.REngine.REXPString;
import org.rosuda.REngine.Rserve.RConnection;

import com.nexr.rhive.hive.HiveVariations;
import com.nexr.rhive.util.EnvUtils;

/**
 * RUDF
 * 
 */
@Description(name = "R", value = "_FUNC_(export-name,arg1,arg2,...,return-type) - Returns the result of R scalar function")
public class RUDF extends GenericUDF {
	private static Configuration conf = new Configuration();
	private static Set<String> funcSet = new HashSet<String>();
	private static String NULL = "";
	private static int STRING_TYPE = 1;
	private static int NUMBER_TYPE = 0;
	private static RConnection rconnection;

	private Converter[] converters;
	private int[] types;

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {

		String function_name = converters[0].convert(arguments[0].get()).toString();

		loadRObjects(function_name);

		StringBuffer argument = new StringBuffer();

		for (int i = 1; i < (arguments.length - 1); i++) {

			Object value = converters[i].convert(arguments[i].get());

			if (value == null) {
				argument.append("NULL");
			} else {
				if (types[i] == STRING_TYPE) {
					argument.append("\"" + converters[i].convert(arguments[i].get()) + "\"");
				} else {
					argument.append(converters[i].convert(arguments[i].get()));
				}
			}

			if (i < (arguments.length - 2))
				argument.append(",");
		}

		REXP rdata = null;
		try {
			rdata = getConnection().eval(function_name + "(" + argument.toString() + ")");
		} catch (Exception e) {
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			e.printStackTrace(new PrintStream(output));
			throw new HiveException(new String(output.toByteArray())
					+ " -- fail to eval : " + function_name + "("
					+ argument.toString() + ")");
		}

		if (rdata != null) {
			try {
				if (rdata instanceof REXPInteger) {
					return new IntWritable(rdata.asInteger());
				} else if (rdata instanceof REXPString) {
					return new Text(rdata.asString());
				} else if (rdata instanceof REXPDouble) {
					return new DoubleWritable(rdata.asDouble());
				} else {
					throw new HiveException(
							"only support integer, string and double");
				}
			} catch (Exception e) {
				ByteArrayOutputStream output = new ByteArrayOutputStream();
				e.printStackTrace(new PrintStream(output));
				throw new HiveException(new String(output.toByteArray()));
			}
		}

		return null;
	}

	@Override
	public String getDisplayString(String[] children) {
		StringBuilder sb = new StringBuilder();
		sb.append("Rfunction(");
		for (int i = 0; i < children.length; i++) {
			sb.append(children[i]);
			if (i + 1 != children.length) {
				sb.append(",");
			}
		}
		sb.append(")");
		return sb.toString();
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {

		GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
		returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(
				true);

		for (int i = 0; i < arguments.length; i++) {
			if (!returnOIResolver.update(arguments[i])) {
				throw new UDFArgumentTypeException(i, "Argument type \""
						+ arguments[i].getTypeName()
						+ "\" is different from preceding arguments. "
						+ "Previous type was \""
						+ arguments[i - 1].getTypeName() + "\"");
			}
		}

		converters = new Converter[arguments.length];
		types = new int[arguments.length];

		ObjectInspector returnOI = returnOIResolver.get();
		if (returnOI == null) {
			returnOI = PrimitiveObjectInspectorFactory
					.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING);
		}
		for (int i = 0; i < arguments.length; i++) {
			converters[i] = ObjectInspectorConverters.getConverter(
					arguments[i], returnOI);
			if (arguments[i].getCategory() == Category.PRIMITIVE
					&& ((PrimitiveObjectInspector) arguments[i])
							.getPrimitiveCategory() == PrimitiveCategory.STRING)
				types[i] = STRING_TYPE;
			else
				types[i] = NUMBER_TYPE;
		}

		String typeName = arguments[arguments.length - 1].getTypeName();

		try {
			if (typeName.equals(HiveVariations.getFieldValue(HiveVariations.serdeConstants, "INT_TYPE_NAME"))) {
				return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
			} else if (typeName.equals(HiveVariations.getFieldValue(HiveVariations.serdeConstants, "DOUBLE_TYPE_NAME"))) {
				return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
			} else if (typeName.equals(HiveVariations.getFieldValue(HiveVariations.serdeConstants, "STRING_TYPE_NAME"))) {
				return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
			} else
				throw new IllegalArgumentException("can't support this type : " + typeName);
		} catch (Exception e) {
			throw new UDFArgumentException(e);
		}
	}

	private void loadExportedRScript(String export_name) throws HiveException {
		if (!funcSet.contains(export_name)) {

			try {
				REXP rhive_data = getConnection().eval(
						"Sys.getenv('RHIVE_DATA')");
				String srhive_data = null;

				if (rhive_data != null) {
					srhive_data = rhive_data.asString();
				}

				if (srhive_data == null || srhive_data == ""
						|| srhive_data.length() == 0) {

					getConnection().eval(
							"load(file=paste('/tmp','/" + export_name + ".Rdata',sep=''))");
				} else {

					getConnection().eval(
							"load(file=paste(Sys.getenv('RHIVE_DATA'),'/" + export_name + ".Rdata',sep=''))");
				}

			} catch (Exception e) {

				ByteArrayOutputStream output = new ByteArrayOutputStream();
				e.printStackTrace(new PrintStream(output));
				throw new HiveException(new String(output.toByteArray()));
			}

			funcSet.add(export_name);
		}
	}

	private void loadRObjects(String name) throws HiveException {
		if (!funcSet.contains(name)) {
			try {
				FileSystem fs = FileSystem.get(conf);
				
				boolean srcDel = false;
				Path src = UDFUtils.getPath(name);
				Path dst = getLocalPath(name);
				fs.copyToLocalFile(srcDel, src, dst);
				
				String dataFilePath = dst.toString();
				getConnection().eval(String.format("load(file=\"%s\")", dataFilePath));

			} catch (Exception e) {
				throw new HiveException(e);
			}
				
			funcSet.add(name);
		}
	}

	private Path getLocalPath(String name) {
		String tempDirectory = EnvUtils.getTempDirectory();
		return new Path(tempDirectory, UDFUtils.getFileName(name));
	}
	
	private RConnection getConnection() throws UDFArgumentException {
		if (rconnection == null || !rconnection.isConnected()) {
			try {
				rconnection = new RConnection("127.0.0.1");
			} catch (Exception e) {
				ByteArrayOutputStream output = new ByteArrayOutputStream();
				e.printStackTrace(new PrintStream(output));
				throw new UDFArgumentException(new String(output.toByteArray()));
			}
		}

		return rconnection;
	}
}
