\name{rhive-api}
\alias{rhive.list.table}
\alias{rhive.desc.table}
\alias{rhive.load.table}
\title{R function to handle data and meta info from HIVE}
\usage{
rhive.list.table(hiveclient=rhive.defaults('hiveclient'))
rhive.desc.table(tablename,detail=FALSE, hiveclient=rhive.defaults('hiveclient'))
rhive.load.table(tablename, limit = -1, hiveclient=rhive.defaults('hiveclient'))
}
\arguments{
\item{hiveclient}{a client for hive.}
\item{tablename}{hive table name.}
\item{detail}{flag is whether to show detail of table info or not}
\item{limit}{total fetch size}
} 
\author{
\email{rhive@nexr.com}
}
\examples{
## try to connect hive server
rhive.connect("127.0.0.1")

## get list of tables from hive
rhive.list.table()

## get table info from hive
rhive.desc.table('emp')

## get table detail info from hive
rhive.desc.table('emp',TRUE)

## retrieve data from hive
emp <- rhive.load.table('emp')

## display column names
colnames(emp)

## display row count
length(rownames(emp))

## close connection
rhive.close()
}
\keyword{programming}