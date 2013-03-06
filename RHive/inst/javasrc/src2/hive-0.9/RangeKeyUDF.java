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
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Writable;

import com.nexr.rhive.util.RangeTreeFactory;
import com.nexr.rhive.util.RangeTreeFactory.RangeTree;

public class RangeKeyUDF extends GenericUDF {
    
    private Configuration config;

    private ObjectInspector[] argumentOIs;

    private RANGEVALUE rangeValue;

    private ObjectInspector returnOI;
    
    private String breaks = null;
    private Boolean isRight = null;
    
    private static Map<String, RangeTreeFactory.RangeTree> TREES = new LinkedHashMap<String, RangeTreeFactory.RangeTree>();
    
    public static enum RANGEVALUE {
        INT_TYPE {
            ObjectInspector inspector() {
                return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
            }

            RangeTreeFactory.RangeTree newTree(String name, boolean minExclusive, boolean maxExclusive) {
                return RangeTreeFactory.createIntTree(name, minExclusive, maxExclusive, null);
            }

            RangeTreeFactory.RangeTree newTree(String name, boolean minExclusive, boolean maxExclusive,
                    Object defaultValue) {
                return RangeTreeFactory.createIntTree(name, minExclusive, maxExclusive, defaultValue);
            }

            Integer parse(String value) {
                return Integer.valueOf(value);
            }

            Object asArray(String minValue, String maxValue) {
                return new int[] { parse(minValue).intValue() , parse(maxValue).intValue() };
            }

            Object search(RangeTreeFactory.RangeTree tree, Object value) {
                return ((RangeTreeFactory.IntRangeTree) tree).search((Integer) value);
            }
            
            @SuppressWarnings("unchecked")
            RangeTree init(String minValue, String maxValue, String stepValue, RangeTree tree, boolean minExclusive) {
                
                String left = "(";
                String right = "]";
                
                if(!minExclusive) {
                    left = "[";
                    right = ")";
                }

                int lstart = parse(minValue).intValue();
                int lend = parse(maxValue).intValue();
                int step = parse(stepValue).intValue();
                
                for(int idx = lstart; idx < lend; idx=idx+step) {
                    int[] irange = {idx,idx+step};
                    tree.put(irange, left +irange[0] + "," + irange[1] + right);
                }
                
                return tree;
            }
            
            @SuppressWarnings("unchecked")
            RangeTree init(String[] breaks, RangeTree tree, boolean minExclusive) {
                
                String left = "(";
                String right = "]";
                
                if(!minExclusive) {
                    left = "[";
                    right = ")";
                }
                
                
                for(int i = 1; i < breaks.length; i++) {
                    int lstart = parse(breaks[i-1]).intValue();
                    int lend = parse(breaks[i]).intValue();
                    
                    int[] irange = {lstart,lend};
                    tree.put(irange, left +irange[0] + "," + irange[1] + right);
                }
                
                return tree;
            }

            Writable searchWritable(RangeTreeFactory.RangeTree tree, Object value) {
                return ((RangeTreeFactory.IntRangeTree) tree).searchWritable((Integer) value);
            }
        },
        BIGINT_TYPE {
            ObjectInspector inspector() {
                return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
            }

            RangeTreeFactory.RangeTree newTree(String name, boolean minExclusive, boolean maxExclusive) {
                return RangeTreeFactory.createLongTree(name, minExclusive, maxExclusive, null);
            }

            RangeTreeFactory.RangeTree newTree(String name, boolean minExclusive, boolean maxExclusive,
                    Object defaultValue) {
                return RangeTreeFactory.createLongTree(name, minExclusive, maxExclusive, defaultValue);
            }

            Long parse(String value) {
                return Long.valueOf(value);
            }

            Object asArray(String minValue, String maxValue) {
                return new long[] { parse(minValue).longValue() , parse(maxValue).longValue() };
            }

            Object search(RangeTreeFactory.RangeTree tree, Object value) {
                return ((RangeTreeFactory.LongRangeTree) tree).search((Long) value);
            }
            
            @SuppressWarnings("unchecked")
            RangeTree init(String minValue, String maxValue, String stepValue, RangeTree tree, boolean minExclusive) {
                
                String left = "(";
                String right = "]";
                
                if(!minExclusive) {
                    left = "[";
                    right = ")";
                }
                
                long lstart = parse(minValue).longValue();
                long lend = parse(maxValue).longValue();
                long step = parse(stepValue).longValue();
                
                for(long idx = lstart; idx < lend; idx=idx+step) {
                    long[] irange = {idx,idx+step};
                    tree.put(irange, left + irange[0] + "," + irange[1] + right);
                }
                
                return tree;
            }      
            
            @SuppressWarnings("unchecked")
            RangeTree init(String[] breaks, RangeTree tree, boolean minExclusive) {
                
                String left = "(";
                String right = "]";
                
                if(!minExclusive) {
                    left = "[";
                    right = ")";
                }
                
                for(int i = 1; i < breaks.length; i++) {
                    long lstart = parse(breaks[i-1]).longValue();
                    long lend = parse(breaks[i]).longValue();
                    
                    long[] irange = {lstart,lend};
                    tree.put(irange, left +irange[0] + "," + irange[1] + right);
                }
                
                return tree;
            }

            Writable searchWritable(RangeTreeFactory.RangeTree tree, Object value) {
                return ((RangeTreeFactory.LongRangeTree) tree).searchWritable((Long) value);
            }
        },
        DOUBLE_TYPE {
            ObjectInspector inspector() {
                return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
            }

            RangeTreeFactory.RangeTree newTree(String name, boolean minExclusive, boolean maxExclusive) {
                return RangeTreeFactory.createDoubleTree(name, minExclusive, maxExclusive, null);
            }

            RangeTreeFactory.RangeTree newTree(String name, boolean minExclusive, boolean maxExclusive,
                    Object defaultValue) {
                return RangeTreeFactory.createDoubleTree(name, minExclusive, maxExclusive, defaultValue);
            }

            Double parse(String value) {
                return Double.valueOf(value);
            }

            Object asArray(String minValue, String maxValue) {
                return new double[] { parse(minValue).doubleValue() , parse(maxValue).doubleValue() };
            }

            Object search(RangeTreeFactory.RangeTree tree, Object value) {
                return ((RangeTreeFactory.DoubleRangeTree) tree).search((Double) value);
            }
            
            @SuppressWarnings("unchecked")
            RangeTree init(String minValue, String maxValue, String stepValue, RangeTree tree, boolean minExclusive) {
                
                String left = "(";
                String right = "]";
                
                if(!minExclusive) {
                    left = "[";
                    right = ")";
                }
                
                double lstart = parse(minValue).doubleValue();
                double lend = parse(maxValue).doubleValue();
                double step = parse(stepValue).doubleValue();
                
                for(double idx = lstart; idx < lend; idx=idx+step) {
                    double[] irange = {idx,idx+step};
                    tree.put(irange, left + irange[0] + "," + irange[1] + right);
                }
                
                return tree;
            }          
            
            @SuppressWarnings("unchecked")
            RangeTree init(String[] breaks, RangeTree tree, boolean minExclusive) {
                
                String left = "(";
                String right = "]";
                
                if(!minExclusive) {
                    left = "[";
                    right = ")";
                }
                
                for(int i = 1; i < breaks.length; i++) {
                    double lstart = parse(breaks[i-1]).doubleValue();
                    double lend = parse(breaks[i]).doubleValue();
                    
                    double[] irange = {lstart,lend};
                    tree.put(irange, left +irange[0] + "," + irange[1] + right);
                }
                
                return tree;
            }

            Writable searchWritable(RangeTreeFactory.RangeTree tree, Object value) {
                return ((RangeTreeFactory.DoubleRangeTree) tree).searchWritable((Double) value);
            }
        },
        FLOAT_TYPE {
            ObjectInspector inspector() {
                return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
            }

            RangeTreeFactory.RangeTree newTree(String name, boolean minExclusive, boolean maxExclusive) {
                return RangeTreeFactory.createFloatTree(name, minExclusive, maxExclusive, null);
            }

            RangeTreeFactory.RangeTree newTree(String name, boolean minExclusive, boolean maxExclusive,
                    Object defaultValue) {
                return RangeTreeFactory.createFloatTree(name, minExclusive, maxExclusive, defaultValue);
            }

            Float parse(String value) {
                return Float.valueOf(value);
            }

            Object asArray(String minValue, String maxValue) {
                return new float[] { parse(minValue).floatValue(), parse(maxValue).floatValue() };
            }

            Object search(RangeTreeFactory.RangeTree tree, Object value) {
                return ((RangeTreeFactory.FloatRangeTree) tree).search((Float) value);
            }
            
            @SuppressWarnings("unchecked")
            RangeTree init(String minValue, String maxValue, String stepValue, RangeTree tree, boolean minExclusive) {
                
                String left = "(";
                String right = "]";
                
                if(!minExclusive) {
                    left = "[";
                    right = ")";
                }
                
                float lstart = parse(minValue).floatValue();
                float lend = parse(maxValue).floatValue();
                float step = parse(stepValue).floatValue();
                
                for(float idx = lstart; idx < lend; idx=idx+step) {
                    float[] irange = {idx,idx+step};
                    tree.put(irange, left + irange[0] + "," + irange[1] + right);
                }
                
                return tree;
            }          
            
            @SuppressWarnings("unchecked")
            RangeTree init(String[] breaks, RangeTree tree, boolean minExclusive) {
                
                String left = "(";
                String right = "]";
                
                if(!minExclusive) {
                    left = "[";
                    right = ")";
                }
                
                for(int i = 1; i < breaks.length; i++) {
                    float lstart = parse(breaks[i-1]).floatValue();
                    float lend = parse(breaks[i]).floatValue();
                    
                    float[] irange = {lstart,lend};
                    tree.put(irange, left +irange[0] + "," + irange[1] + right);
                }
                
                return tree;
            }

            Writable searchWritable(RangeTreeFactory.RangeTree tree, Object value) {
                return ((RangeTreeFactory.FloatRangeTree) tree).searchWritable((Float) value);
            }
        },
        STRING_TYPE {
            ObjectInspector inspector() {
                return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
            }

            RangeTreeFactory.RangeTree newTree(String name, boolean minExclusive, boolean maxExclusive) {
                return RangeTreeFactory.createStringTree(name, minExclusive, maxExclusive, null);
            }

            RangeTreeFactory.RangeTree newTree(String name, boolean minExclusive, boolean maxExclusive,
                    Object defaultValue) {
                return RangeTreeFactory.createStringTree(name, minExclusive, maxExclusive, defaultValue);
            }

            String parse(String value) {
                return value;
            }

            Object asArray(String minValue, String maxValue) {
                return new String[] { parse(minValue), parse(maxValue) };
            }

            Object search(RangeTreeFactory.RangeTree tree, Object value) {
                return ((RangeTreeFactory.StringRangeTree) tree).search(String.valueOf(value));
            }
            
            RangeTree init(String minValue, String maxValue, String stepValue, RangeTree tree, boolean minExclusive) {
           
                throw new RuntimeException("can't split min-max for string type.");
            }  
            
            RangeTree init(String[] breaks, RangeTree tree, boolean minExclusive) {
                
                throw new RuntimeException("can't split min-max for string type.");
            }

            Writable searchWritable(RangeTreeFactory.RangeTree tree, Object value) {
                return ((RangeTreeFactory.StringRangeTree) tree).searchWritable(String.valueOf(value));
            }
        };
        abstract ObjectInspector inspector();

        abstract Object parse(String value);

        abstract Object asArray(String minValue, String maxValue);

        @SuppressWarnings("unchecked")
        abstract RangeTree init(String minValue, String maxValue, String step, RangeTree tree, boolean minExclusive);
        
        @SuppressWarnings("unchecked")
        abstract RangeTree init(String[] breaks, RangeTree tree, boolean minExclusive);
        
        abstract Object search(RangeTreeFactory.RangeTree tree, Object value);

        abstract Writable searchWritable(RangeTreeFactory.RangeTree tree, Object value);

        @SuppressWarnings("unchecked")
        abstract RangeTreeFactory.RangeTree newTree(String name, boolean minExclusive,
                boolean maxExclusive);

        @SuppressWarnings("unchecked")
        abstract RangeTreeFactory.RangeTree newTree(String name, boolean minExclusive,
                boolean maxExclusive, Object defaultValue);
    }

    private RANGEVALUE valueOf(String typeName) {
        
        if (typeName.equals(Constants.INT_TYPE_NAME)) {
            return RANGEVALUE.INT_TYPE;
        } else if (typeName.equals(Constants.BIGINT_TYPE_NAME)) {
            return RANGEVALUE.BIGINT_TYPE;
        } else if (typeName.equals(Constants.DOUBLE_TYPE_NAME)) {
            return RANGEVALUE.DOUBLE_TYPE;
        } else if (typeName.equals(Constants.FLOAT_TYPE_NAME)) {
            return RANGEVALUE.FLOAT_TYPE;
        } else if (typeName.equals(Constants.STRING_TYPE_NAME)) {
            return RANGEVALUE.STRING_TYPE;
        }
        throw new IllegalArgumentException("RHive doesn't support this type " + typeName);
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
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
        this.rangeValue = valueOf(valueType);
        this.returnOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;

        return returnOI;
    }

    @Override
    public Object evaluate(DeferredObject[] records) throws HiveException {

        if(breaks == null) {
            breaks = (String) ((PrimitiveObjectInspector) argumentOIs[1])
                    .getPrimitiveJavaObject(records[1].get());
            isRight = new Boolean((String) ((PrimitiveObjectInspector) argumentOIs[1])
                    .getPrimitiveJavaObject(records[2].get()));
        }
        
        RangeTreeFactory.RangeTree tree = TREES.get(breaks);
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
            throw new IllegalArgumentException(
                    "fail to eval : " + e.getMessage(), e);
        }
    }
    
    private RangeTree loadTree() {
        
        String start, end, step;
        String splits;
        
        RangeTree tree = rangeValue.newTree(breaks, isRight.booleanValue() ,!isRight.booleanValue());
        
        if(breaks.indexOf(":") > 0) {
        
            StringTokenizer st = new StringTokenizer(breaks, ":");
            
            if (st.countTokens() == 2) {
                start = st.nextToken();
                end = st.nextToken();
                step = "1";
         
                tree = rangeValue.init(start, end, step, tree, isRight.booleanValue());
            }else if(st.countTokens() == 3) { 
                
                start = st.nextToken();
                end = st.nextToken();
                step = st.nextToken();
                
                tree = rangeValue.init(start, end, step, tree, isRight.booleanValue());
            }else {
                throw new RuntimeException("fail to parse break syntax : " + breaks);
            }
        
        }else if(breaks.indexOf(",") > 0) {
            
            StringTokenizer st = new StringTokenizer(breaks, ",");
            String[] elements = new String[st.countTokens()];
            for(int i = 0; i < elements.length; i++) {
                elements[i] = st.nextToken();
            }

            tree = rangeValue.init(elements, tree, isRight.booleanValue());
        }else {
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
