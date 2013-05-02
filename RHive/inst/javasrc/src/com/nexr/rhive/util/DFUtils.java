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

package com.nexr.rhive.util;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.net.NetUtils;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.Rengine;

public class DFUtils {
    public static ThreadLocal<Rengine> localRengine = new ThreadLocal<Rengine>();

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

    /**
     * @param funclist
     * @param export_name
     * @throws HiveException
     */
    public static void loadExportedRScript(Map<String, String> funclist ,String export_name) throws HiveException {

        if (!funclist.containsKey(export_name)) {

            try {
                REXP rhive_data = getConnection().eval("Sys.getenv('RHIVE_DATA')");
                String srhive_data = null;
                Process p = null;

                if (rhive_data != null) {
                    srhive_data = rhive_data.asString();
                }

                // get R Object file from HDFS
                if(srhive_data == null || srhive_data == "" || srhive_data.length() == 0) {

                    p = Runtime.getRuntime().exec("rm " + "/tmp/" + export_name + ".Rdata");
                    p.waitFor();

                    p = Runtime.getRuntime().exec(System.getenv("HADOOP_HOME") +
                            "/bin/hadoop fs -get " + "/rhive/tmp/" + export_name + ".Rdata" + " /tmp/");
                    p.waitFor();

                    getConnection().eval(
                            "load(file=paste('/tmp','/" + export_name
                                    + ".Rdata',sep=''))");

                }else {

                    ProcessRunner proc = new ProcessRunner(
                            new String[]{"rm", srhive_data + "/" + export_name + ".Rdata"},
                            System.getenv("R_HOME") + "/bin/");

                    proc.start();
                    proc.join();

                    p = Runtime.getRuntime().exec("rm " + srhive_data + "/" + export_name + ".Rdata");
                    p.waitFor();

                    p = Runtime.getRuntime().exec(System.getenv("HADOOP_HOME") +
                            "/bin/hadoop fs -get " + "/rhive/tmp/" + export_name + ".Rdata" + " " + srhive_data + "/");
                    p.waitFor();

                    getConnection().eval(
                            "load(file=paste('" + srhive_data + "','/" + export_name
                                    + ".Rdata',sep=''))");
                }

            } catch (Exception e) {

                ByteArrayOutputStream output = new ByteArrayOutputStream();
                e.printStackTrace(new PrintStream(output));
                throw new HiveException(new String(output.toByteArray()));
            }

            funclist.put(export_name, "");
        }
    }


    public static Rengine getConnection() throws HiveException {
        System.out.println("getConnection()"+Thread.currentThread().getName());
        if (localRengine.get() == null) {

            System.setProperty("jri.ignore.ule", "yes");

            if (System.getenv("R_HOME") == null)
                throw new HiveException("R_HOME must be set in hadoop-env.sh");

            try {

                ProcessRunner proc = new ProcessRunner(
                        new String[]{"Rscript", "-e", "cat(system.file('jri', package='rJava'))"},
                        System.getenv("R_HOME") + "/bin/");

                proc.start();
                proc.join();

                int exitVal = proc.getExitCode();

                String libPath = proc.getOutput();

                if (exitVal != 0 || libPath.compareTo("") == 0) {
                    throw new Exception("fail to get JRI library Path");
                }

                System.load( libPath + "/" + "libjri.so");

            } catch (Exception e) {
                throw new HiveException("cannot load JRI library " + e.getMessage());
            }

            if (!Rengine.versionCheck()) {
                throw new HiveException("REngine Version mismatch - Java files don't match library version.");
            }

            localRengine.set(new Rengine(new String[]{"--no-save"}, false, null));

            if (!localRengine.get().waitForR()) {
                throw new HiveException("cannot load R");
            }
        }
        System.out.println("getConnection()_exit"+Thread.currentThread().getName());

        return localRengine.get();
    }

    /**
     * @warn this function is not thread safe
     */
     static class ProcessRunner extends Thread {
        InputStream isError;
        InputStream isOutput;

        String result = "";
        Process process = null;

        public ProcessRunner(String cmd[], String dir) throws IOException {

            ProcessBuilder processBuilder = new ProcessBuilder();
            processBuilder.command(cmd);
            processBuilder.directory(new File(dir));

            process = processBuilder.start();
            isOutput = process.getInputStream();
            isError = process.getErrorStream();
        }

        public int getExitCode(){
            return process.exitValue();
        }

        public synchronized String getOutput() {
            synchronized (result) {
                return result;
            }
        }

        public void run() {

            try {
                InputStreamReader isrOutput = new InputStreamReader(isOutput);
                BufferedReader brOutput = new BufferedReader(isrOutput);

                InputStreamReader isrError = new InputStreamReader(isError);
                BufferedReader brError = new BufferedReader(isrError);

                String line = "";
                do {
                    while ((line = brOutput.readLine()) != null) {
                        synchronized (result) {
                            result += line;
                        }
                    }

                    while ((line = brError.readLine()) != null) {
                        //empty buffer prevent from hanging
                    }
                } while (brError.ready() == true || brOutput.ready() == true);

                process.waitFor();

            } catch (Exception ioe) {
                ioe.printStackTrace();
            }
        }
    }
}
