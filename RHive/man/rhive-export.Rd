\name{rhive-export}
\alias{rhive.export}
\alias{rhive.exportAll}
\title{Export R function to Hive using functions in Package `RHive'}
\usage{
rhive.export(exportname,hosts="127.0.0.1",port=6311,limit = 104857600)
rhive.exportAll(exportname,hosts="127.0.0.1",port=6311,limit = 104857600)
}
\arguments{
\item{hosts}{list of Rserve's address}
\item{query}{hive query.}
\item{exportname}{function name to be exported.}
\item{limit}{total exported object size. default is 100MB}
} 
\author{
\email{rhive@nexr.com}
}
\examples{
## try to connect hive server
rhive.connect("127.0.0.1")

## execute HQL(hive query)
rhive.query("select * from emp")


## define R function
coff <- 5.2
scoring <- function(sal) {
	coff * sal
}

## export R objects (scoring and coff) to Hive 
rhive.export('scoring')

## execute HQL using exported R objects
## name of UDF is 'R'
rhive.query(hivecon,"select R('scoring',sal,0.0) from emp")

## define R aggregation function
## define iterate operator
hsum <- function(prev,sal) {
	c(prev[1] + sal[1])
}
## define partial aggregation operator
hsum.partial <- function(agg_sal) {
	agg_sal
}
## define merge operator
hsum.merge <- function(prev, agg_sal) {
	c(prev[1] + agg_sal[1])
}
## define final aggregation operator
hsum.terminate <- function(agg_sal) {
	agg_sal
}

rhive.exportAll('hsum')

## name of UDAF is 'RA'
rhive.query(hivecon,"select RA('hsum',sal) from emp group by empno")

## close connection
rhive.close()
}
\keyword{programming}