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

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class ScaleUDF extends GenericUDF {
    
    Converter[]           converters;
    DoubleObjectInspector scaleFieldOI;
    
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        
        double value = Double.parseDouble(converters[0].convert(arguments[0].get()).toString());
   
        double average = Double.parseDouble(converters[1].convert(arguments[1].get()).toString());
        double std = Double.parseDouble(converters[2].convert(arguments[2].get()).toString());
        
        if(std != 0) {
            double scale = (value - average) / std;
            return new DoubleWritable(scale);
        }else {
            return null;
        }
    }
    
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        
        if (arguments.length != 3) {
            throw new UDFArgumentLengthException("The function scale accepts exactly 3 arguments.");
        }
        
        for (int i = 0; i < arguments.length; i++) {
            Category category = arguments[i].getCategory();
            if (category != Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(i, "The " + GenericUDFUtils.getOrdinal(i + 1)
                        + " argument of function LOCATE is expected to a "
                        + Category.PRIMITIVE.toString().toLowerCase() + " type, but "
                        + category.toString().toLowerCase() + " is found");
            }
        }
        
        converters = new ObjectInspectorConverters.Converter[arguments.length];

        for (int i = 0; i < arguments.length; i++) {
            String typeName = arguments[i].getTypeName();
            
            if (typeName.equals(Constants.INT_TYPE_NAME)) {
                converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
                        PrimitiveObjectInspectorFactory.writableIntObjectInspector);
            } else if (typeName.equals(Constants.DOUBLE_TYPE_NAME)) {
                converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
                        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
            } else if (typeName.equals(Constants.FLOAT_TYPE_NAME)) {
                converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
                        PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
            } else if (typeName.equals(Constants.BIGINT_TYPE_NAME)) {
                converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
                        PrimitiveObjectInspectorFactory.writableLongObjectInspector);
            } else
                throw new IllegalArgumentException("can't support this type : " + typeName);
        }
        
        scaleFieldOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
        
        return scaleFieldOI;
    }
    
    public String getDisplayString(String[] children) {
        StringBuilder sb = new StringBuilder();
        sb.append("scale(");
        for (int i = 0; i < children.length; i++) {
            sb.append(children[i]);
            if (i + 1 != children.length) {
                sb.append(",");
            }
        }
        sb.append(")");
        return sb.toString();
    }
    
}
