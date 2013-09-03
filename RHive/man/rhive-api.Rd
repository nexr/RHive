\name{rhive-api}
\alias{rhive.list.databases}
\alias{rhive.show.databases}
\alias{rhive.use.database}
\alias{rhive.list.tables}
\alias{rhive.show.tables}
\alias{rhive.desc.table}
\alias{rhive.load.table}
\alias{rhive.load.table2}
\alias{rhive.exist.table}
\alias{rhive.size.table}
\alias{rhive.drop.table}
\alias{rhive.set}
\alias{rhive.unset}
\title{R functions to get informations of table from HIVE}
\usage{
rhive.list.databases(pattern)
rhive.show.databases(pattern)
rhive.use.database(databaseName)
rhive.list.tables(pattern)
rhive.show.tables(pattern)
rhive.desc.table(tableName, detail=FALSE)
rhive.load.table(tableName, fetchSize=50, limit=-1)
rhive.load.table2(tableName, limit=-1, remote=TRUE)
rhive.exist.table(tableName) 
rhive.size.table(tableName)
rhive.drop.table(tableName, list)
rhive.set(key, value)
rhive.unset(key)
}
\description{
R functions to get informations of table from HIVE
}
\arguments{
\item{databaseName}{hive database name.}
\item{tableName}{hive table name.}
\item{remote}{hiveserver mode.}
\item{detail}{a flag on whether to show detail of table info.}
\item{limit}{total fetch size. -1 means full fetch}
\item{fetchSize}{the count of record to load at one time}
\item{pattern}{an optional regular expression. Only names 
  matching 'pattern' are returned. 'glob2rx' can be used to
  convert wildcard patterns to regular expressions.}
\item{list}{a character vector naming tables to be removed.
  or rhive.list.tables's result.}          
\item{key}{hive configuration key}
\item{value}{hive configuration value}
} 
\author{
\email{rhive@nexr.com}
}
\examples{
## try to connect hive server
\dontrun{rhive.connect("hive-server-ip")}

## get list of databases in the Hive
\dontrun{rhive.list.databases()}

## set current database
\dontrun{rhive.use.database('default')}

## get list of tables in the Hive
\dontrun{rhive.list.tables()}

## get table info in the Hive
\dontrun{rhive.desc.table('emp')}

## get detail information of a table in the Hive
\dontrun{rhive.desc.table('emp', TRUE)}

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
