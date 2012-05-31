package com.nexr.rhive.hive.udf;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.TaskExecutionException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

@Description(name = "array2String", value = "_FUNC_(a) - convert the elements of array a to string,"
        + " or the elements of a map to string. ")
public class GenericUDFArrayToString extends GenericUDF {
    
    private ObjectInspector inputOI = null;
    
    public Object evaluate(DeferredObject[] o) throws HiveException {
      
        switch (inputOI.getCategory()) {
            case LIST:
                ListObjectInspector listOI = (ListObjectInspector) inputOI;
                List<?> list = listOI.getList(o[0].get());
                if (list == null) {
                    return "[]";
                }else {
                    return new Text(list.toString());
                }
            case MAP:
                MapObjectInspector mapOI = (MapObjectInspector) inputOI;
                Map<?, ?> map = mapOI.getMap(o[0].get());
                if (map == null) {
                    return "[]";
                }else {
                    return new Text(map.toString());
                }
            default:
                throw new TaskExecutionException(
                        "array2String() can only operate on an array or a map");
        }
    }
    
    public String getDisplayString(String[] args) {
        
        StringBuffer display = new StringBuffer();
        
        display.append("array2String(");
        
        for(int i = 0; i < args.length; i++) {
            display.append(args[i]);
            
            if(i < args.length -1 )
                display.append(",");
        }
        
        return display.toString();
    }
    
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        
        if (arguments.length != 1) {
            throw new UDFArgumentException("array2String() takes only one argument");
        }
        
        switch (arguments[0].getCategory()) {
            case LIST:
                inputOI = arguments[0];
                break;
            case MAP:
                inputOI = arguments[0];
                break;
            default:
                throw new UDFArgumentException(
                        "array2String() takes an array or a map as a parameter");
        }
        
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }
}
