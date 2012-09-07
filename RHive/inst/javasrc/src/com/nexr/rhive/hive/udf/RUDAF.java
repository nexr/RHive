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

import java.io.*;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;
import org.rosuda.JRI.*;
import static com.nexr.rhive.util.DFUtils.*;

/**
 * RUDAF
 *
 */
@Description(name = "RA", value = "_FUNC_(export-name,arg1,arg2,...) - Returns the result of R aggregation function")
public class RUDAF extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {

        for (int i = 0; i < parameters.length; i++) {
            switch (((PrimitiveTypeInfo) parameters[i]).getPrimitiveCategory()) {
                case BYTE:
                case SHORT:
                case INT:
                case LONG:
                case FLOAT:
                case DOUBLE:
                case STRING:
                case BOOLEAN:
                    continue;
                default:
                    throw new UDFArgumentTypeException(0,
                            "Only primitive type arguments are accepted but "
                                    + parameters[i].getTypeName() + " is passed.");
            }
        }
        
        return new GenericRUDAF();
    }

    /**
     * GenericRUDAF.
     *
     */
    public static class GenericRUDAF extends GenericUDAFEvaluator {

        private static Map<String, String> funclist    = new Hashtable<String, String>();
        private static String              NULL        = "";
        private static int                 STRING_TYPE = 1;
        private static int                 NUMBER_TYPE = 0;
        
        private Converter[]                converters;
        private int[]                      types;
        private PrimitiveObjectInspector[] inputOIs;
        
        // For FINAL and COMPLETE
        private Text                       result;
        
        // For PARTIAL1 and COMPLETE
        private PrimitiveObjectInspector   inputOI;
        
        // For PARTIAL2 and FINAL
        private StructObjectInspector      soi;
        private StructField                resultField;
        private StructField                fnNameField;
        private StringObjectInspector      resultFieldOI;
        private StringObjectInspector      fnNameFieldOI;
        
        // For PARTIAL1 and PARTIAL2
        private Object[]                   partialResult;
        
        boolean                            warned      = false;
        
        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] arguments) throws HiveException {
            super.init(mode, arguments);

            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
                
                GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
                returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
                
                for (int i = 0; i < arguments.length; i++) {
                    if (!returnOIResolver.update(arguments[i])) {
                        throw new UDFArgumentTypeException(i, "Argument type \""
                                + arguments[i].getTypeName()
                                + "\" is different from preceding arguments. "
                                + "Previous type was \"" + arguments[i - 1].getTypeName() + "\"");
                    }
                }
                
                converters = new Converter[arguments.length];
                
                types = new int[arguments.length];
                inputOIs = new PrimitiveObjectInspector[arguments.length];
                
                ObjectInspector returnOI = returnOIResolver.get();
                if (returnOI == null) {
                    returnOI = PrimitiveObjectInspectorFactory
                            .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
                }
                for (int i = 0; i < arguments.length; i++) {
                    converters[i] = ObjectInspectorConverters.getConverter(arguments[i], returnOI);
                    if (arguments[i].getCategory() == Category.PRIMITIVE
                            && ((PrimitiveObjectInspector) arguments[i]).getPrimitiveCategory() == PrimitiveCategory.STRING) {
                        types[i] = STRING_TYPE;
                        inputOIs[i] = (PrimitiveObjectInspector) arguments[i];
                    } else {
                        types[i] = NUMBER_TYPE;
                        inputOIs[i] = (PrimitiveObjectInspector) arguments[i];
                    }
                }
                
            } else {
                soi = (StructObjectInspector) arguments[0];
                
                resultField = soi.getStructFieldRef("result");
                fnNameField = soi.getStructFieldRef("fn_name");
                resultFieldOI = (StringObjectInspector) resultField.getFieldObjectInspector();
                fnNameFieldOI = (StringObjectInspector) fnNameField.getFieldObjectInspector();
            }
            
            // init output
            if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
                ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
                
                foi.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
                
                ArrayList<String> fname = new ArrayList<String>();
                fname.add("result");
                fname.add("fn_name");
                
                partialResult = new Object[2];
                partialResult[0] = new Text("");
                partialResult[1] = new Text("");
                
                return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
                
            } else {
                
                result = new Text("");
                return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
            }
        }
        
        /** class for storing exportName and values. */
        static class RResultAgg implements AggregationBuffer {

            boolean empty;
            
            String  funcName;
            String  values;
        }
        
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            RResultAgg result = new RResultAgg();
            reset(result);
            return result;
        }
        
        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            RResultAgg myagg = (RResultAgg) agg;
            myagg.funcName = "";
            myagg.empty = true;
            myagg.values = null;
        }
        
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {

            String function_name = PrimitiveObjectInspectorUtils.getString(parameters[0],
                    inputOIs[0]);

            loadExportedRScript(funclist,function_name);

            try {
                RResultAgg myagg = (RResultAgg) agg;
                myagg.empty = false;
                myagg.funcName = function_name;
                
                StringBuffer argument = new StringBuffer();
                if (myagg.values == null) {
                    argument.append("NULL, c(");
                } else {
                    argument.append(myagg.values + ",c(");
                }

                for (int i = 1; i < parameters.length; i++) {
                    
                    if (types[i] == STRING_TYPE) {
                        
                        String value = PrimitiveObjectInspectorUtils.getString(parameters[i],inputOIs[i]);
                        
                        if(value == null) {
                            argument.append("NULL");
                        }else {
                            argument.append("\"" + value + "\"");
                        }
                    } else {
                        
                        if(parameters[i] == null) {
                            argument.append("NULL");
                        } else {
                            double value = PrimitiveObjectInspectorUtils.getDouble(parameters[i],inputOIs[i]);
                            argument.append(value);
                        }
                    }
                    
                    if (i < (parameters.length - 1))
                        argument.append(",");
                }

                REXP rdata = null;
                try {
                    rdata = getConnection().eval(function_name + "(" + argument.toString() + "))");
                } catch (Exception e) {
                    ByteArrayOutputStream output = new ByteArrayOutputStream();
                    e.printStackTrace(new PrintStream(output));
                    throw new HiveException(new String(output.toByteArray()) + " -- fail to eval : " + function_name + "(" + argument.toString()
                            + ")");
                }
                
                if (rdata != null) {
                    tranformR2Hive(myagg, rdata, false);
                }

            } catch (NumberFormatException e) {
                if (!warned) {
                    warned = true;
                }
            }
        }
        
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {

            try {
                RResultAgg myagg = (RResultAgg) agg;
                String function_name = myagg.funcName + ".partial";
                
                REXP rdata = null;
                try {
                    if (myagg.values == null) {
                        rdata = getConnection().eval(function_name + "(NULL)");
                    } else {
                        rdata = getConnection()
                                .eval(function_name + "(" + myagg.values + ")");
                    }
                } catch (Exception e) {
                    ByteArrayOutputStream output = new ByteArrayOutputStream();
                    e.printStackTrace(new PrintStream(output));
                    throw new HiveException(new String(output.toByteArray()) + " -- fail to eval : " + function_name + "(" + myagg.values + ")");
                }


                if (rdata != null) {
                    tranformR2Hive(myagg, rdata, false);
                }

                ((Text) partialResult[0]).set(myagg.values);
                ((Text) partialResult[1]).set(myagg.funcName);
                
            } catch (NumberFormatException e) {
                if (!warned) {
                    warned = true;
                    ByteArrayOutputStream output = new ByteArrayOutputStream();
                    e.printStackTrace(new PrintStream(output));
                    throw new HiveException(new String(output.toByteArray()));
                }
            }
            
            return partialResult;
        }
        
        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {

            if (partial != null) {
                RResultAgg myagg = (RResultAgg) agg;
                
                Object partialResult = soi.getStructFieldData(partial, resultField);
                String partial_value = resultFieldOI.getPrimitiveJavaObject(partialResult);
                
                if (myagg.funcName == null || myagg.funcName.equals("")) {
                    Object partialFnName = soi.getStructFieldData(partial, fnNameField);
                    myagg.funcName = fnNameFieldOI.getPrimitiveJavaObject(partialFnName);
                }
 
                myagg.empty = false;
                
                try {
                    loadExportedRScript(funclist, myagg.funcName);
                    
                    String function_name = myagg.funcName + ".merge";
                    
                    REXP rdata = null;
                    try {
                        if (myagg.values == null) {
                            rdata = getConnection().eval(
                                    function_name + "(NULL," + partial_value + ")");
                        } else {
                            rdata = getConnection().eval(
                                    function_name + "(" + myagg.values + "," + partial_value
                                            + ")");
                        }
                    } catch (Exception e) {
                        ByteArrayOutputStream output = new ByteArrayOutputStream();
                        e.printStackTrace(new PrintStream(output));
                        throw new HiveException(new String(output.toByteArray()) + " -- fail to eval : " + function_name + "("
                                + myagg.values + "," + partial_value + ")");
                    }
                    
                    if (rdata != null) {
                        tranformR2Hive(myagg, rdata, false);
                    }
                    
                } catch (NumberFormatException e) {
                    if (!warned) {
                        warned = true;
                        ByteArrayOutputStream output = new ByteArrayOutputStream();
                        e.printStackTrace(new PrintStream(output));
                        throw new HiveException(new String(output.toByteArray()));
                    }
                }
                
            }
        }
        
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            RResultAgg myagg = (RResultAgg) agg;

            if (myagg.empty) {
                return null;
            }
            try {
                String function_name = myagg.funcName + ".terminate";
                
                REXP rdata = null;
                try {
                    if (myagg.values == null) {
                        rdata = getConnection().eval(function_name + "(NULL)");
                    } else {
                        rdata = getConnection()
                                .eval(function_name + "(" + myagg.values + ")");
                    }
                } catch (Exception e) {
                    ByteArrayOutputStream output = new ByteArrayOutputStream();
                    e.printStackTrace(new PrintStream(output));
                    throw new HiveException(new String(output.toByteArray()) + " -- fail to eval : " + function_name + "("
                            + myagg.values + ")");
                }
                
                if (rdata != null) {
                    tranformR2Hive(myagg, rdata, true);
                }

                result.set(myagg.values);

            } catch (NumberFormatException e) {
                if (!warned) {
                    warned = true;
                    ByteArrayOutputStream output = new ByteArrayOutputStream();
                    e.printStackTrace(new PrintStream(output));
                    throw new HiveException(new String(output.toByteArray()));
                }
            }
            
            return result;
        }
        


        private void tranformR2Hive(RResultAgg myagg, REXP rdata, boolean isTerminate)
                throws HiveException {
            try {
                StringBuffer sb = new StringBuffer();

                appendRData(sb,rdata,isTerminate);

                myagg.values = sb.toString();

            } catch (Exception e) {
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                e.printStackTrace(new PrintStream(output));
                throw new HiveException(new String(output.toByteArray()));
            }
        }

        private void appendRData(StringBuffer sb,REXP rdata , boolean isTerminate) throws HiveException {

            int type = rdata.getType();
            // why only return type Vector, not List
            if (type == REXP.XT_VECTOR || type == REXP.XT_LIST) {
                RList list = rdata.asList();

                handleList(list, sb, isTerminate);

            } else if (type == REXP.XT_ARRAY_BOOL || type == REXP.XT_ARRAY_DOUBLE || type == REXP.XT_ARRAY_INT || type == REXP.XT_ARRAY_STR) {

                handleArray(rdata, sb, isTerminate);

            } else if (type == REXP.XT_STR) {
                sb.append("'" + rdata.asString() + "'");
            } else if (type == REXP.XT_DOUBLE) {
                sb.append(Double.toString(rdata.asDouble()));
            } else if (type == REXP.XT_INT) {
                sb.append(Integer.toString(rdata.asInt()));
            } else if (type == REXP.XT_BOOL) {
                sb.append(((rdata.asInt() == 1 )? "TRUE" : "FALSE"));
            } else {
                throw new HiveException("not support this type : " + rdata.toString());
            }
        }

        private void handleList(RList rlist, StringBuffer sb, boolean isTerminate)
                throws HiveException {

            if (!isTerminate)
                sb.append("list(");

            for (int i = 0;i < rlist.keys().length; i++) {

                REXP rdata = rlist.at(i);

                appendRData(sb,rdata,isTerminate);

                if (i < (rlist.keys().length - 1))
                    sb.append(",");
            }

            if (!isTerminate)
                sb.append(")");
        }

        private void handleArray(REXP array, StringBuffer sb, boolean isTerminate)
                throws HiveException {
            // all elements of vector is double and string.
            if (!isTerminate)
                sb.append("c(");

            int type = array.getType();

            if (type == REXP.XT_ARRAY_BOOL){

                int [] elements = array.asIntArray();
                for (int i = 0; i < elements.length; i++){
                    sb.append((elements[i] == 1 )? "TRUE" : "FALSE");
                    if (i < (elements.length - 1))
                        sb.append(",");
                }

            } else if (type == REXP.XT_ARRAY_DOUBLE) {

                double [] elements = array.asDoubleArray();
                for (int i = 0; i < elements.length; i++){
                    sb.append(Double.toString(elements[i]));
                    if (i < (elements.length - 1))
                        sb.append(",");
                }

            } else if (type == REXP.XT_ARRAY_INT) {

                int [] elements = array.asIntArray();
                for (int i = 0; i < elements.length; i++){
                    sb.append(Integer.toString(elements[i]));
                    if (i < (elements.length - 1))
                        sb.append(",");
                }

            } else if (type == REXP.XT_ARRAY_STR) {

                String [] elements = array.asStringArray();
                for (int i = 0; i < elements.length; i++){
                    sb.append("'"+elements[i]+"'");
                    if (i < (elements.length - 1))
                        sb.append(",");
                }
            } else {
                throw new HiveException(
                        "only support numeric and string in vector");
            }

            if (!isTerminate)
                sb.append(")");
        }

    }
}
