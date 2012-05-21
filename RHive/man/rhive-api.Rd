\name{rhive-api}
\alias{rhive.list.tables}
\alias{rhive.desc.table}
\alias{rhive.load.table}
\alias{rhive.load.table2}
\alias{rhive.exist.table}
\alias{rhive.size.table}
\alias{rhive.drop.table}
\title{R functions to get informations of table from HIVE}
\usage{
rhive.list.tables(pattern, hiveclient=rhive.defaults('hiveclient'))
rhive.desc.table(tablename,detail=FALSE, hiveclient=rhive.defaults('hiveclient'))
rhive.load.table(tablename, fetchsize = 40, limit = -1, hiveclient=rhive.defaults('hiveclient'))
rhive.load.table2(tablename, remote = TRUE, hiveclient=rhive.defaults('hiveclient'))
rhive.exist.table(tablename, hiveclient=rhive.defaults('hiveclient')) 
rhive.size.table(tablename, hiveclient =rhive.defaults('hiveclient'))
rhive.drop.table(tablename, list, hiveclient =rhive.defaults('hiveclient'))
}
\description{
R functions to get informations of table from HIVE
}
\arguments{
\item{hiveclient}{a client for hive.}
\item{tablename}{hive table name.}
\item{remote}{hiveserver mode.}
\item{detail}{a flag on whether to show detail of table info.}
\item{limit}{total fetch size. -1 means full fetch}
\item{fetchsize}{the count of record to load at one time}
\item{pattern}{an optional regular expression. Only names matching 
'pattern' are returned. 'glob2rx' can be used to convert 
wildcard patterns to regular expressions.}
\item{list}{a character vector naming tables to be removed. or rhive.list.tables's result.}          
} 
\author{
\email{rhive@nexr.com}
}
\examples{
## try to connect hive server
\dontrun{rhive.connect("hive-server-ip")}

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
