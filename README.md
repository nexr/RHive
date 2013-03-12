NexR RHive 0.0-7
================

  RHive is an R extension facilitating distributed computing via HIVE query.
  RHive allows easy usage of HQL(Hive SQL) in R, and allows easy usage of R objects and R functions in Hive.

> Before installing RHive, you has to have installed Hadoop and Hive

## Install Hadoop
1. Single Node
    - [Single node installation](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html)
2. Cluster Node
    - [Cluster node installation](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/ClusterSetup.html)
3. set **HADOOP_HOME** at local machine on which R runs

## Install Hive
1. install local machine and remote machine on which NameNode runs or Hive-Server runs.
2. Installation Guide 
    - [Hive installation guide](https://cwiki.apache.org/confluence/display/Hive/GettingStarted#GettingStarted-InstallationandConfiguration)
3. set **HIVE_HOME** at local machine on which R runs.
4. launch Hive Server with following command on remote machine. it should be as a background process.
    - <code>$HIVE_HOME/bin/hive --service hiveserver</code>

## Install R and Packages
1. install R
    - need to install R on all tasktracker nodes
2. install rJava
    - only install rJava on local machine.
3. Rserve mode - install Rserve
    - need to install Rserve on all tasktracker nodes
    - set **RHIVE_DATA** as R objects and R functions repository on all tasktracker nodes. if **RHIVE_DATA** is not set then it will be '/tmp' as a default.
        - e.g> <code>export RHIVE_DATA=/rhive/data</code>
    - make configuration in path (/etc/Rserv.conf) on all tasktracker nodes.
         edit this file to add 'remote enable' to allow remote connection.
    - launch all Rserve on all tasktracker nodes.
        - e.q> <code>R CMD Rserve</code>
4. No Rserve mode - setting tasktracker nodes (Optional)
    1. set RHIVE_DATA as R objects and R functions repository on all tasktracker nodes.
        - e.q> <code>export RHIVE_DATA=/rhive/data</code>
    2. add R_HOME path at $HADOOP_HOME/conf/hadoop-env.sh
        - e.q> <code>export R_HOME=/usr/lib/R</code>
5. install RUnit

## Install RHive
1. Requirements
    - ant (in order to build jar files)
2. Installing RHive
    1. Compressed package: <code>R CMD INSTALL RHive_0.0-7.tar.gz</code>
    2. Source code: <code>R CMD INSTALL ./RHive</code>
3. If **HADOOP_HOME** doesn't exist, do following instruction :
    1. copy RUDF/RUDAF library(rhive_udf.jar) to '/rhive/lib/' of HDFS path, 
      using this command : 'hadoop fs -put rhive_udf.jar /rhive/lib/rhive_udf.jar'. 
    this jar file exists under $HIVE_HOME/lib. 

## Loading RHive and connecting to Hive
1. launch R
<pre><code>library(RHive)
rhive.connect(hive-server-ip)</code></pre>
  
## Tutorials
- [RHive user guide](https://github.com/nexr/RHive/wiki/UserGuides)

## Requirements
- Java 1.6
- R 2.13.0
- Rserve 0.6-0
- rJava 0.9-0
- Hadoop 0.20.x (x >= 1)
- Hive 0.8.x (x >= 0)
