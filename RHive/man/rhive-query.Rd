\name{rhive-query}
\alias{rhive.query}
\title{Execute HQL(Hive Query) in R, using functions in Package `RHive'}
\usage{
rhive.query(query, fetchsize = 40, limit = -1, hiveclient=rhive.defaults('hiveclient'))
}
\description{
Execute HQL(Hive Query) in R using functions in Package `RHive'
}
\arguments{
\item{query}{hive query.}
\item{fetchsize}{fetch size for result.}
\item{limit}{total result size.}
\item{hiveclient}{a client for hive.}
} 
\details{
The query argument is Hive Query Language. 
}
\value{
\code{rhive.query} returns data.frame.
}
\references{
Apache Hive Query Language Manual
(\url{https://cwiki.apache.org/confluence/display/Hive/LanguageManual}).
}
\author{
\email{rhive@nexr.com}
}
\examples{
## try to connect hive server
\dontrun{rhive.connect("127.0.0.1")}

## execute hive query
\dontrun{rhive.query("select ename from emp")}

## close connection
\dontrun{rhive.close()}
}
\keyword{programming}
