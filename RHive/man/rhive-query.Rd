\name{rhive-query}
\alias{hiveQuery}
\title{Execute HQL(Hive Query) in R using functions in Package `RHive'}
\usage{
hiveQuery(query, fetchsize = 40, limit = -1, hiveclient=rhive.defaults('hiveclient'))
}
\arguments{
\item{query}{hive query.}
\item{fetchsize}{fetch size for result.}
\item{limit}{total result size.}
\item{hiveclient}{a client for hive.}
} 
\author{
\email{rhive@nexr.com}
}
\details{

}
\examples{
## try to connect hive server
rhive.connect("127.0.0.1")

## execute hive query
rhive.query("select enam from emp")

## close connection
rhive.close()
}
\keyword{programming}