package com.nexr.rhive.hadoop;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by bruceshin on 11/19/14.
 */
public class AuthUtils {

    public static void loginUserFromKeytab(Properties properties, String service) throws IOException{

        String principal = properties.getProperty(service + ".principal");
        String keytab = properties.getProperty(service + ".keytab");

        if(StringUtils.isEmpty(principal) || StringUtils.isEmpty(keytab)){
            return;
        }

        UserGroupInformation.loginUserFromKeytab(principal, keytab);
    }

    public static void setConfigurationUserGroup(Configuration conf){
        UserGroupInformation.setConfiguration(conf);
    }
}
