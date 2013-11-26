NexR RHive 2.0-0.0
================

  RHive is an R extension facilitating distributed computing via HIVE query.
  RHive allows easy usage of HQL(Hive SQL) in R, and allows easy usage of R objects and R functions in Hive.

> Before installing RHive, you have to have installed Hadoop and Hive

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
3. install Rserve
    - need to install Rserve on all tasktracker nodes
    - make configuration in path (/etc/Rserv.conf) on all tasktracker nodes.
         edit this file to add 'remote enable' to allow remote connection.
    - launch all Rserve on all tasktracker nodes.
        - e.q> <code>R CMD Rserve</code>
4. setting tasktracker nodes
    - add R_HOME path at $HADOOP_HOME/conf/hadoop-env.sh
        - e.q> <code>export R_HOME=/usr/lib/R</code>
5. install RUnit

## Install RHive
1. Requirements
    - ant (in order to build java files)
2. Installing RHive
    1. Download source code: <code>git clone https://github.com/nexr/RHive.git</code>
    2. Change your working directory: <code>cd RHive</code> 
    3. Set the environment variables HIVE_HOME and HADOOP_HOME: <code>export HIVE_HOME=/path/to/your/hive/directory</code> <code>export HADOOP_HOME=/path/to/your/hadoop/directory</code>
    5. Build java files using ant: <code>ant build</code>
    4. Build RHive: <code>R CMD build RHive</code>
    5. Install RHive: <code>R CMD INSTALL RHive_<VERSION>.tar.gz</code>

## Loading RHive and connecting to Hive
1. launch R
<pre><code>library(RHive)
rhive.connect(host, port, hiveServer2)</code></pre>
  
## Tutorials
- [RHive user guide](https://github.com/nexr/RHive/wiki/User-Guide)

## Requirements
- Java 1.6
- R 2.13.0
- Rserve 0.6-0
- rJava 0.9-0
- Hadoop 0.20.x (x >= 1)
- Hive 0.8.x (x >= 0)
