\name{rhive-export}
\alias{rhive.export}
\alias{rhive.exportAll}
\title{Export R function to Hive using functions in Package `RHive'}
\usage{
rhive.export(exportname,hosts = "127.0.0.1", port = 6311, pos = 1, envir = .rhiveExportEnv, limit = 104857600)
rhive.exportAll(exportname, hosts = "127.0.0.1", port = 6311, pos = 1, envir = .rhiveExportEnv, limit = 104857600)
rhive.assign(name,value)
rhive.rm(name)
}
\arguments{
\item{hosts}{list of Rserve's address}
\item{query}{hive query.}
\item{exportname}{function name to be exported.}
\item{limit}{total exported object size. default is 100MB}
\item{name}{a variable name, given as a character string.  No coercion is done, 
			and the first element of a character vector of length
            greater than one will be used, with a warning.}
\item{value}{a value to be assigned to ¡®x¡¯.}
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

## assign R object to Hive
rhive.assign('scoring',scoring)
rhive.assign('coff',coff)

## export R objects (scoring and coff) to Hive 
rhive.exportAll('scoring')

## execute HQL using exported R objects
## name of UDF is 'R'
rhive.query("select R('scoring',sal,0.0) from emp")

## delete R object in .rhiveExportEnv
rhive.rm('scoring')
rhive.rm('coff')

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

rhive.assign('hsum',hsum)
rhive.assign('hsum.partial',hsum.partial)
rhive.assign('hsum.merge',hsum.merge)
rhive.assign('hsum.terminate',hsum.terminate)
rhive.exportAll('hsum')

## name of UDAF is 'RA'
rhive.query(hivecon,"select RA('hsum',sal) from emp group by empno")

## close connection
rhive.close()
}
\keyword{programming}