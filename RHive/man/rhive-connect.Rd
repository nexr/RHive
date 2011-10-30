\name{rhive-connect}
\alias{rhive.init}
\alias{rhive.connect}
\alias{rhive.close}
\title{Manage connection to Hive using functions in Package `RHive'}
\usage{
rhive.init(hive=NULL,libs=NULL,verbose=FALSE)
rhive.connect(host,port=10000)
rhive.close(hiveclient=rhive.defaults('hiveclient'))
}
\description{
Manage connection to Hive using functions in Package `RHive'
}
\arguments{
\item{hive}{path of hive's installation or if HIVE_HOME is set, it is possible to use as NULL.}
\item{libs}{library path to be added to classpath.}
\item{host}{hive-server address for connecting to hive.}
\item{port}{hive-server listen port.}
\item{hiveclient}{a client for hive.}
\item{verbose}{an option on whether to print detail message.}
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
