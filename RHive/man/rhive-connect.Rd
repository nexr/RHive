\name{rhive-connect}
\alias{rhive.init}
\alias{rhive.env}
\alias{rhive.connect}
\alias{rhive.close}
\title{Manage connection to Hive using functions in Package `RHive'}
\usage{
rhive.init(hive=NULL,libs=NULL,hadoop=NULL, hlibs=NULL, verbose=FALSE)
rhive.env(ALL=FALSE)
rhive.connect(host="127.0.0.1",port=10000, hdfsurl=NULL ,hosts = rhive.defaults('slaves'))
rhive.close(hiveclient=rhive.defaults('hiveclient'))
}
\description{
Manage connection to Hive using functions in Package `RHive'
}
\arguments{
\item{hive}{path of hive's installation or if HIVE_HOME is set, it is possible to use as NULL.}
\item{hadoop}{path of hadoop's installation or if HADOOP_HOME is set, it is possible to use as NULL.}
\item{libs}{library path to be added to classpath.}
\item{hlibs}{hadoop library path to be added to classpath.}
\item{host}{hive-server address for connecting to hive.}
\item{hosts}{list of Rserve's address}
\item{port}{hive-server listen port.}
\item{hiveclient}{a client for hive.}
\item{hdfsurl}{the url of hdfs namenode.}
\item{verbose}{an option on whether to print detail message.}
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
