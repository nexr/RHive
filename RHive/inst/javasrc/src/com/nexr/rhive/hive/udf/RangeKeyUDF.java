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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Writable;

import com.nexr.rhive.hive.HiveVariations;
import com.nexr.rhive.util.RangeTreeFactory;
import com.nexr.rhive.util.RangeTreeFactory.RangeTree;

public class RangeKeyUDF extends GenericUDF {

	private Configuration config;

	private ObjectInspector[] argumentOIs;

	private RANGEVALUE rangeValue;

	private ObjectInspector returnOI;

	private String breaks = null;
	private Boolean isRight = null;

	private static Map<String, RangeTree> TREES = new LinkedHashMap<String, RangeTree>();

	public static enum RANGEVALUE {
		INT_TYPE {
			ObjectInspector inspector() {
				return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
			}

			Object search(RangeTree tree, Object value) {
				return ((RangeTreeFactory.DoubleRangeTree) tree)
						.search(((Integer) value).doubleValue());
			}

			Writable searchWritable(RangeTree tree, Object value) {
				return ((RangeTreeFactory.DoubleRangeTree) tree)
						.searchWritable(((Integer) value).doubleValue());
			}
		},
		BIGINT_TYPE {
			ObjectInspector inspector() {
				return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
			}

			Object search(RangeTree tree, Object value) {
				return ((RangeTreeFactory.DoubleRangeTree) tree)
						.search(((Long) value).doubleValue());
			}

			Writable searchWritable(RangeTree tree, Object value) {
				return ((RangeTreeFactory.DoubleRangeTree) tree)
						.searchWritable(((Long) value).doubleValue());
			}
		},
		DOUBLE_TYPE {
			ObjectInspector inspector() {
				return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
			}

			Object search(RangeTree tree, Object value) {
				return ((RangeTreeFactory.DoubleRangeTree) tree)
						.search((Double) value);
			}

			Writable searchWritable(RangeTree tree, Object value) {
				return ((RangeTreeFactory.DoubleRangeTree) tree)
						.searchWritable((Double) value);
			}
		},
		FLOAT_TYPE {
			ObjectInspector inspector() {
				return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
			}

			Object search(RangeTree tree, Object value) {
				return ((RangeTreeFactory.DoubleRangeTree) tree)
						.search(((Float) value).doubleValue());
			}

			Writable searchWritable(RangeTree tree, Object value) {
				return ((RangeTreeFactory.DoubleRangeTree) tree)
						.searchWritable(((Float) value).doubleValue());
			}
		},
		STRING_TYPE {
			ObjectInspector inspector() {
				return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
			}

			RangeTree newTree(String name, boolean minExclusive,
					boolean maxExclusive) {
				return RangeTreeFactory.createStringTree(name, minExclusive, maxExclusive, null);
			}

			RangeTree newTree(String name, boolean minExclusive,
					boolean maxExclusive, Object defaultValue) {
				return RangeTreeFactory.createStringTree(name, minExclusive, maxExclusive, defaultValue);
			}

			Object asArray(String minValue, String maxValue) {
				return new String[] { minValue, maxValue };
			}

			Object search(RangeTree tree, Object value) {
				return ((RangeTreeFactory.StringRangeTree) tree).search(String.valueOf(value));
			}

			RangeTree init(String minValue, String maxValue, String stepValue, RangeTree tree, boolean minExclusive) {
				throw new RuntimeException("can't split min-max for string type.");
			}

			RangeTree init(String[] breaks, RangeTree tree, boolean minExclusive) {
				throw new RuntimeException("can't split min-max for string type.");
			}

			Writable searchWritable(RangeTree tree, Object value) {
				return ((RangeTreeFactory.StringRangeTree) tree).searchWritable(String.valueOf(value));
			}
		};
		abstract ObjectInspector inspector();

		@SuppressWarnings("unchecked")
		RangeTree init(String minValue, String maxValue, String stepValue,
				RangeTree tree, boolean minExclusive) {

			String left = "(";
			String right = "]";

			if (!minExclusive) {
				left = "[";
				right = ")";
			}

			double lstart = parse(minValue).doubleValue();
			double lend = parse(maxValue).doubleValue();
			double step = parse(stepValue).doubleValue();

			for (double idx = lstart; idx < lend; idx = idx + step) {
				double[] irange = { idx, idx + step };
				tree.put(irange, left + String.format("%f", irange[0]) + ","
						+ String.format("%f", irange[1]) + right);
			}

			return tree;
		}

		@SuppressWarnings("unchecked")
		RangeTree init(String[] breaks, RangeTree tree, boolean minExclusive) {

			String left = "(";
			String right = "]";

			if (!minExclusive) {
				left = "[";
				right = ")";
			}

			for (int i = 1; i < breaks.length; i++) {
				double lstart = parse(breaks[i - 1]).doubleValue();
				double lend = parse(breaks[i]).doubleValue();

				double[] irange = { lstart, lend };
				tree.put(irange, left + breaks[i - 1] + "," + breaks[i] + right);
			}

			return tree;
		}

		RangeTree newTree(String name, boolean minExclusive,
				boolean maxExclusive) {
			return RangeTreeFactory.createDoubleTree(name, minExclusive,
					maxExclusive, null);
		}

		RangeTree newTree(String name, boolean minExclusive,
				boolean maxExclusive, Object defaultValue) {
			return RangeTreeFactory.createDoubleTree(name, minExclusive,
					maxExclusive, defaultValue);
		}

		Double parse(String value) {
			return Double.valueOf(value);
		}

		Object asArray(String minValue, String maxValue) {
			return new double[] { parse(minValue).doubleValue(),
					parse(maxValue).doubleValue() };
		}

		abstract Object search(RangeTree tree, Object value);

		abstract Writable searchWritable(RangeTree tree, Object value);

	}

	private RANGEVALUE valueOf(String typeName)
			throws IllegalArgumentException, SecurityException,
			IllegalAccessException, NoSuchFieldException {

		if (typeName.equals(HiveVariations.getFieldValue(HiveVariations.serdeConstants, "INT_TYPE_NAME"))) {
			return RANGEVALUE.INT_TYPE;
		} else if (typeName.equals(HiveVariations.getFieldValue(HiveVariations.serdeConstants, "BIGINT_TYPE_NAME"))) {
			return RANGEVALUE.BIGINT_TYPE;
		} else if (typeName.equals(HiveVariations.getFieldValue(HiveVariations.serdeConstants, "DOUBLE_TYPE_NAME"))) {
			return RANGEVALUE.DOUBLE_TYPE;
		} else if (typeName.equals(HiveVariations.getFieldValue(HiveVariations.serdeConstants, "FLOAT_TYPE_NAME"))) {
			return RANGEVALUE.FLOAT_TYPE;
		} else if (typeName.equals(HiveVariations.getFieldValue(HiveVariations.serdeConstants, "STRING_TYPE_NAME"))) {
			return RANGEVALUE.STRING_TYPE;
		}
		throw new IllegalArgumentException("RHive doesn't support this type " + typeName);
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {
		if (config == null) {
			SessionState session = SessionState.get();
			config = session == null ? new Configuration() : session.getConf();
		}

		if (arguments.length < 3) {
			throw new UDFArgumentLengthException(
					"The function rkey(column, breaks, right) needs at least three arguments.");
		}

		String valueType = arguments[0].getTypeName();

		this.argumentOIs = arguments;
		try {
			this.rangeValue = valueOf(valueType);
		} catch (Exception e) {
			throw new UDFArgumentException(e);
		}
		this.returnOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;

		return returnOI;
	}

	@Override
	public Object evaluate(DeferredObject[] records) throws HiveException {

		if (breaks == null) {
			breaks = (String) ((PrimitiveObjectInspector) argumentOIs[1])
					.getPrimitiveJavaObject(records[1].get());
			isRight = new Boolean(
					(String) ((PrimitiveObjectInspector) argumentOIs[1])
							.getPrimitiveJavaObject(records[2].get()));
		}

		RangeTree tree = TREES.get(breaks);
		if (tree == null) {
			TREES.put(breaks, tree = loadTree());
		}
		Object value = ((PrimitiveObjectInspector) argumentOIs[0])
				.getPrimitiveJavaObject(records[0].get());
		try {
			return rangeValue.searchWritable(tree, value);
		} catch (NullPointerException e) {
			return null;
		} catch (Exception e) {
			throw new IllegalArgumentException("fail to eval : "
					+ e.getMessage(), e);
		}
	}

	private RangeTree loadTree() {

		String start, end, step;
		String splits;

		RangeTree tree = rangeValue.newTree(breaks, isRight.booleanValue(),
				!isRight.booleanValue());

		if (breaks.indexOf(":") > 0) {

			StringTokenizer st = new StringTokenizer(breaks, ":");

			if (st.countTokens() == 2) {
				start = st.nextToken();
				end = st.nextToken();
				step = "1";

				tree = rangeValue.init(start, end, step, tree,
						isRight.booleanValue());
			} else if (st.countTokens() == 3) {

				start = st.nextToken();
				end = st.nextToken();
				step = st.nextToken();

				tree = rangeValue.init(start, end, step, tree,
						isRight.booleanValue());
			} else {
				throw new RuntimeException("fail to parse break syntax : " + breaks);
			}

		} else if (breaks.indexOf(",") > 0) {

			StringTokenizer st = new StringTokenizer(breaks, ",");
			String[] elements = new String[st.countTokens()];
			for (int i = 0; i < elements.length; i++) {
				elements[i] = st.nextToken();
			}

			tree = rangeValue.init(elements, tree, isRight.booleanValue());
		} else {
			throw new RuntimeException("fail to parse break syntax : " + breaks);
		}

		return tree;
	}

	public String getDisplayString(String[] children) {
		StringBuilder sb = new StringBuilder();
		sb.append("rkey (");
		for (int i = 0; i < children.length - 1; i++) {
			sb.append(children[i]).append(", ");
		}
		sb.append(children[children.length - 1]).append(")");
		return sb.toString();
	}
}
