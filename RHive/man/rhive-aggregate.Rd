\name{rhive.aggregate}
\alias{rhive.aggregate}
\title{R Distributed aggregate function using HQL}
\usage{
rhive.aggregate(tableName, hiveFUN, ..., groups=NULL,
  forcedRef=TRUE)
}
\description{
R Distributed aggregate function using HQL
}
\arguments{
\item{tableName}{hive table name.}
\item{hiveFUN}{the hive buit-in function name to be applied.}
\item{...}{optional arguments to 'hiveFUN'.}
\item{groups}{aggregated key list. it is vector type}
\item{forcedRef}{the option which forces to create temp-table
  for result.}
} 
\author{
\email{rhive@nexr.com}
}
\examples{
## try to connect hive server
\dontrun{rhive.connect("hive-server-ip")}

## invoke napply for numeric return type
\dontrun{rhive.aggregate('emp', 'sum', 'sal', c('ename'))}

## close connection
\dontrun{rhive.close()}
}
\keyword{programming}
