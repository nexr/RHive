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
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPDouble;
import org.rosuda.REngine.REXPGenericVector;
import org.rosuda.REngine.REXPInteger;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REXPString;
import org.rosuda.REngine.REXPVector;
import org.rosuda.REngine.RList;
import org.rosuda.REngine.Rserve.RConnection;

/**
 * RUDAF
 *
 */
@Description(name = "RA", value = "_FUNC_(x) - Returns the result of R aggregation function")
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
        
        private static RConnection         rconnection;
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
            
            loadExportedRScript(function_name);
            
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
                        argument.append("\""
                                + PrimitiveObjectInspectorUtils.getString(parameters[i],
                                        inputOIs[i]) + "\"");
                    } else {
                        argument.append(PrimitiveObjectInspectorUtils.getDouble(parameters[i],
                                inputOIs[i]));
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
                    throw new HiveException(new String(output.toByteArray()) + " -- fail to eval : " + function_name + "(" + myagg.values
                            + ")");
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
                    loadExportedRScript(myagg.funcName);
                    
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
        
        private RConnection getConnection() throws UDFArgumentException {
            if (rconnection == null || !rconnection.isConnected()) {
                try {
                    rconnection = new RConnection("127.0.0.1");
                } catch (Exception e) {
                    throw new UDFArgumentException(e.toString());
                }
            }
            
            return rconnection;
        }
        
        /**
         * @param export_name
         * @throws HiveException
         */
        private void loadExportedRScript(String export_name) throws HiveException {
            if (!funclist.containsKey(export_name)) {
                
                try {
                    getConnection().eval(
                            "load(file=paste(Sys.getenv('RHIVE_DATA'),'/" + export_name
                                    + ".Rdata',sep=''))");
                } catch (Exception e) {
                    ByteArrayOutputStream output = new ByteArrayOutputStream();
                    e.printStackTrace(new PrintStream(output));
                    throw new HiveException(new String(output.toByteArray()));
                }
                
                funclist.put(export_name, NULL);
            }
        }
        
        private void tranformR2Hive(RResultAgg myagg, REXP rdata, boolean isTerminate)
                throws HiveException {
            try {
                
                // why rserve only return REXPGenericVector, not REXPList
                if (rdata instanceof REXPGenericVector) {
                    StringBuffer sb = new StringBuffer();
                    REXPGenericVector list = (REXPGenericVector) rdata;
                    
                    handleList(list, sb, isTerminate);
                    
                    myagg.values = sb.toString();
                    
                } else if (rdata instanceof REXPVector) {
                    StringBuffer sb = new StringBuffer();
                    REXPVector vector = (REXPVector) rdata;
                    
                    handleVector(vector, sb, isTerminate);
                    
                    myagg.values = sb.toString();
                    
                } else if (rdata instanceof REXPString) {
                    myagg.values = "'" + rdata.asString() + "'";
                } else if (rdata instanceof REXPDouble) {
                    myagg.values = Double.toString(rdata.asDouble());
                } else if (rdata instanceof REXPInteger) {
                    myagg.values = Integer.toString(rdata.asInteger());
                } else {
                    throw new HiveException("not support this type : " + rdata.toString());
                }
            } catch (Exception e) {
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                e.printStackTrace(new PrintStream(output));
                throw new HiveException(new String(output.toByteArray()));
            }
        }

        private void handleList(REXPGenericVector list, StringBuffer sb, boolean isTerminate)
                throws REXPMismatchException, HiveException {
            RList rlist = list.asList();
            
            if (!isTerminate)
                sb.append("list(");
            
            for (int i = 0; i < rlist.size(); i++) {
                
                Object result = rlist.get(i);
                
                if (result instanceof REXPVector) {
                    
                    REXPVector vector = (REXPVector) result;
                    handleVector(vector, sb, isTerminate);
                    
                } else if (result instanceof REXPString) {
                    sb.append("'" + ((REXPString) result).asString() + "'");
                } else if (result instanceof REXPDouble) {
                    sb.append(Double.toString(((REXPDouble) result).asDouble()));
                } else if (result instanceof REXPInteger) {
                    sb.append(Integer.toString(((REXPInteger) result).asInteger()));
                } else {
                    throw new HiveException(
                            "only support vector, string, double and integer in List");
                }
                
                if (i < (rlist.size() - 1))
                    sb.append(",");
            }
            
            if (!isTerminate)
                sb.append(")");
        }

        private void handleVector(REXPVector vector, StringBuffer sb, boolean isTerminate)
                throws REXPMismatchException, HiveException {
            // all elements of vector is double and string.
            if (!isTerminate)
                sb.append("c(");
            
            if (vector.isNumeric()) {
                // convert all numeric data to double.
                double[] values = vector.asDoubles();
                for (int j = 0; j < values.length; j++) {
                    sb.append(Double.toString(values[j]));
                    if (j < (values.length - 1))
                        sb.append(",");
                }
            } else if (vector.isString()) {
                String[] values = vector.asStrings();
                for (int j = 0; j < values.length; j++) {
                    sb.append("'" + values[j] + "'");
                    if (j < (values.length - 1))
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
