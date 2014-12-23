\name{rhive-connect}
\alias{rhive.init}
\alias{rhive.env}
\alias{rhive.connect}
\alias{rhive.close}
\title{Manage connection to Hive using functions in Package `RHive'}
\usage{
rhive.init(hiveHome=NULL, hiveLib=NULL, hadoopHome=NULL, hadoopConf=NULL,
  hadoopLib=NULL, verbose=FALSE)
rhive.env(ALL=FALSE)
rhive.connect(host="127.0.0.1",port=10000, hiveServer2=NA, defaultFS=NULL,
  updateJar=FALSE, user=NULL, password=NULL)
rhive.close()
}
\description{
Create and manage connections to Hive as part of the RHive package.
}
\arguments{
\item{hiveHome}{path of hive's installation. If HIVE_HOME is set,
  this does not have to be specified - otherwise, this is a required parameter.}
\item{hadoopHome}{path of hadoop's installation. If HADOOP_HOME is set,
  this does not have to be specified - otherwise, this is a required parameter.}
\item{hadoopConf}{path to hadoop's configuation directory. If HADOOP_CONF_DIR is set,
  this does not have to be specified - otherwise, this is a required parameter.}
\item{hiveLib}{library path to be added to classpath.}
\item{hadoopLib}{hadoop library path to be added to classpath.}
\item{host}{hive-server address for connecting to hive.}
\item{port}{hive-server listen port.}
\item{hiveServer2}{TRUE if you are using HiveServer2 and FALSE otherwise.}
\item{defaultFS}{the url of hdfs namenode.}
\item{updateJar}{update rhive_udf.jar}
\item{user}{the username for the query to run as.}
\item{password}{the user's password}
\item{verbose}{Whether to print detailed messages or not.}
\item{ALL}{show all rhive enviroment, such as classpath.}
} 
\author{
\email{rhive@nexr.com}
}
\examples{
## initialize rhive
\dontrun{rhive.init()}

## try to connect hive server
\dontrun{rhive.connect("127.0.0.1")}

## close connection
\dontrun{rhive.close()}
}
\keyword{programming}
