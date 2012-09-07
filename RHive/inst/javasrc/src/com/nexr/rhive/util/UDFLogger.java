package com.nexr.rhive.util;

import java.util.Date;

/**
 * @author tim.yang
 */
public class UDFLogger {
    public static boolean debugEnabled = false;
    public static boolean infoEnabled = false;

    public static void printDebugLog(String log){
        if (debugEnabled == false) return;
        System.out.println(new Date().toString() + ":" + log);
    }

    public static void printInfoLog(String log){
        if (infoEnabled == false) return;
        System.out.println(new Date().toString() + ":" + log);
    }
}
