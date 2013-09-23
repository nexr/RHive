\name{rhive-execute}
\alias{rhive.execute}
\title{Execute HQL(Hive Query) in R, using functions in Package `RHive'}
\usage{
rhive.execute(query)
}
\description{
Execute HQL(Hive Query) in R using functions in Package `RHive'
}
\arguments{
\item{query}{hive query.}
} 
\details{
The query argument is Hive Query Language. 
}
\value{
\code{rhive.execute} returns TRUE on success.
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
\dontrun{rhive.execute("create table emp (empno int, 
                                          ename string, 
                                          position string, 
                                          col int,
                                          fday string, 
                                          sal double,
                                          status string, 
                                          deptno int)")}

## close connection
\dontrun{rhive.close()}
}
\keyword{programming}
