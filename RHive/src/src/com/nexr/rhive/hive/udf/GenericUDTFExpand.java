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
import org.apache.hadoop.hive.serde.Constants;
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

/**
 * GenericUDTFExpand
 * 
 */
@Description(name = "expand", value = "_FUNC_(x) - Returns the extended result of one column")
public class GenericUDTFExpand extends GenericUDTF {
    
    enum DATA_TYPE {
        STRING, DOUBLE, INT, NUMERIC, NULLNAME
    };
    
    DATA_TYPE         data_type;         // mapping from data-type to enum DATA_TYPE
    Writable[]        retRow;            // returned row value but only use one.
    Writable[]        row;               // object pool of non-null writable, avoid creating
                                          // objects all the time
    Object[]          nullRow;           // array of null row value
    ObjectInspector[] inputOIs;          // input ObjectInspectors
    boolean           pathParsed = false;
    boolean           seenErrors = false;
    String            delim      = ",";
    
    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        inputOIs = args;
        
        if (args.length < 2 || args.length > 3) {
            throw new UDFArgumentException("expand() takes 2 or 3 arguments: "
                    + "the origin string, data type and delim");
        }
        
        for (int i = 0; i < args.length; ++i) {
            if (args[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentException("expand()'s arguments have to be primitive type");
            }
        }
        
        seenErrors = false;
        pathParsed = false;
        
        row = new Writable[1];
        retRow = new Writable[1];
        nullRow = new Object[1];
        
        // construct output object inspector
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        // column name can be anything since it will be named by UDTF as clause
        fieldNames.add("col");
        // all returned type will be primitive type
        fieldOIs.add(getColumnInspector(args[1].getTypeName()));
        row[0] = getColumnWritable(args[1].getTypeName());
        retRow[0] = row[0];
        nullRow = null;
        
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }
    
    private ObjectInspector getColumnInspector(String typeName) throws IllegalArgumentException {
        if (typeName.equals(Constants.INT_TYPE_NAME)) {
            return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
        } else if (typeName.equals(Constants.DOUBLE_TYPE_NAME)) {
            return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
        } else if (typeName.equals(Constants.STRING_TYPE_NAME)) {
            return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        } else
            throw new IllegalArgumentException("can't support this type " + typeName);
    }
    
    private Writable getColumnWritable(String typeName) throws IllegalArgumentException {
        if (typeName.equals(Constants.INT_TYPE_NAME)) {
            return new IntWritable(0);
        } else if (typeName.equals(Constants.DOUBLE_TYPE_NAME)) {
            return new DoubleWritable(0.0);
        } else if (typeName.equals(Constants.STRING_TYPE_NAME)) {
            return new Text();
        } else
            throw new IllegalArgumentException("can't support this type : " + typeName);
    }
    
    @Override
    public void process(Object[] args) throws HiveException {
        if (args[0] == null) {
            forward(nullRow);
            return;
        }
        
        // get the path names for the 1st row only
        if (!pathParsed) {
            if (inputOIs[1] instanceof StringObjectInspector) {
                data_type = DATA_TYPE.STRING;
            } else if (inputOIs[1] instanceof DoubleObjectInspector) {
                data_type = DATA_TYPE.DOUBLE;
            } else if (inputOIs[1] instanceof IntObjectInspector) {
                data_type = DATA_TYPE.INT;
            } else {
                data_type = DATA_TYPE.NULLNAME;
                throw new HiveException("we don't know this type... " + inputOIs[1]);
            }
            
            if (args.length == 3) {
                String local_delim = ((StringObjectInspector) inputOIs[2])
                        .getPrimitiveJavaObject(args[args.length - 1]);
                if (local_delim != null && !local_delim.equals(""))
                    delim = local_delim;
            }
            
            pathParsed = true;
        }
        
        String originStr = ((StringObjectInspector) inputOIs[0]).getPrimitiveJavaObject(args[0]);
        if (originStr == null) {
            forward(nullRow);
            return;
        }
        
        try {
            String[] originStrs = parseString(originStr, delim);
            for (int i = 0; i < originStrs.length; ++i) {
                
                if (originStrs[i] == null) {
                    retRow[0] = row[0]; // use the object pool rather than creating a new object
                } else {
                    switch (data_type) {
                        case STRING:
                            ((Text) retRow[0]).set(originStrs[i]);
                            break;
                        case DOUBLE:
                            ((DoubleWritable) retRow[0]).set(Double.parseDouble(originStrs[i]));
                            break;
                        case INT:
                            ((IntWritable) retRow[0]).set((int) Double.parseDouble(originStrs[i]));
                            break;
                        case NULLNAME:
                        default:
                            retRow = row;
                    }
                }
                
                forward(retRow);
            }
            
            return;
        } catch (Exception e) {
            if (!seenErrors) {
                seenErrors = true;
            }
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            e.printStackTrace(new PrintStream(output));
            throw new HiveException(new String(output.toByteArray()));
            
         // forward(nullRow);
         // return;
        }
    }
    
    public String toString() {
        return "expand";
    }
    
    /**
     * Called to notify the UDTF that there are no more rows to process. Clean up code or additional
     * forward() calls can be made here.
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
