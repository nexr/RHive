package com.nexr.rhive.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;

public class DFUtils {
    
    private Configuration conf;
    
    public DFUtils(Configuration conf) throws Exception {
        this.conf = conf;
    }
    
    public Configuration getConf() {
        return this.conf;
    }
    
    @SuppressWarnings("unchecked")
    private String getInfoServer() throws IOException {
        return NetUtils.getServerAddress(getConf(), "dfs.info.bindAddress", "dfs.info.port",
                "dfs.http.address");
    }
    
    public String[] getFileInfo(String dir) throws Exception {
        String fsName = getInfoServer();
        StringBuffer url = new StringBuffer("http://" + fsName + "/fsck?path=");
        
        Path hdfs = new Path(dir);
        dir = hdfs.toUri().getPath();
        
        url.append(URLEncoder.encode(dir, "UTF-8"));
        // url.append("&blocks=1");
        
        URL path = new URL(url.toString());
        URLConnection connection = path.openConnection();
        InputStream stream = connection.getInputStream();
        BufferedReader input = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
        String line = null;
        List<String> metas = new ArrayList<String>();
        
        try {
            while ((line = input.readLine()) != null) {
                StringTokenizer tokens = new StringTokenizer(line, "\t");
                if (tokens.countTokens() == 2) {
                    // skip key part.
                    String key = tokens.nextToken();
                    if (key.trim().startsWith("Total blocks")) {
                        StringTokenizer _tokens = new StringTokenizer(tokens.nextToken(), " ");
                        metas.add(_tokens.nextToken());
                    } else {
                        metas.add(tokens.nextToken());
                    }
                }
            }
        } finally {
            input.close();
        }
        return metas.toArray(new String[0]);
    }
    
}
