package com.nexr.rhive.util;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.fs.FileStatus;

public class TransformUtils {
    
    private static final SimpleDateFormat formatter =  new SimpleDateFormat("yyyy-MM-dd HH:mm");
    
    public static String[] tranform(FileStatus fs) throws Exception {
        
        String[] datas = new String[6];
        
        datas[0] = fs.getPermission().toString();
        datas[1] = fs.getOwner();
        datas[2] = fs.getGroup();
        datas[3] = Long.toString(fs.getLen());
        
        Date date = new Date(fs.getModificationTime());
        datas[4] = formatter.format(date);
        datas[5] = fs.getPath().toUri().getPath();

        return datas;
    }
    
}
