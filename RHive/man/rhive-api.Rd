\name{rhive-api}
\alias{rhive.list.tables}
\alias{rhive.desc.table}
\alias{rhive.load.table}
\title{R functions to get informations of table from HIVE}
\usage{
rhive.list.tables(hiveclient=rhive.defaults('hiveclient'))
rhive.desc.table(tablename,detail=FALSE, hiveclient=rhive.defaults('hiveclient'))
rhive.load.table(tablename, limit = -1, hiveclient=rhive.defaults('hiveclient'))
}
\description{
R functions to get informations of table from HIVE
}
\arguments{
\item{hiveclient}{a client for hive.}
\item{tablename}{hive table name.}
\item{detail}{a flag on whether to show detail of table info.}
\item{limit}{total fetch size}
} 
\author{
\email{rhive@nexr.com}
}
\examples{
## try to connect hive server
\dontrun{rhive.connect("127.0.0.1")}

## get list of tables from hive
\dontrun{rhive.list.tables()}

## get table info from hive
\dontrun{rhive.desc.table('emp')}

## get table detail info from hive
\dontrun{rhive.desc.table('emp',TRUE)}

## retrieve data from hive
\dontrun{emp <- rhive.load.table('emp')}

## display column names
\dontrun{colnames(emp)}

## display row count
\dontrun{length(rownames(emp))}

## close connection
\dontrun{rhive.close()}
}
\keyword{programming}
