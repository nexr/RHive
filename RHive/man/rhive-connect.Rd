\name{rhive-connect}
\alias{rhive.connect}
\alias{rhive.close}
\title{Manage connection to Hive using functions in Package `RHive'}
\usage{
rhive.connect(host,port=10000)
rhive.close(hiveclient=rhive.defaults('hiveclient'))
}
\arguments{
\item{host}{hive-server address to connect hive.}
\item{port}{hive-server listen port.}
\item{hiveclient}{a client for hive.}
} 
\author{
\email{rhive@nexr.com}
}
\examples{
## try to connect hive server
rhive.connect("127.0.0.1")

## close connection
rhive.close()
}
\keyword{programming}