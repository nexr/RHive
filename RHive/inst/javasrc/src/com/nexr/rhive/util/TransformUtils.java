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
