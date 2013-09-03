\name{rhive-apply}
\alias{rhive.napply}
\alias{rhive.sapply}
\alias{rhive.mrapply}
\alias{rhive.mapapply}
\alias{rhive.reduceapply}
\title{R Distributed apply function using HQL}
\usage{
rhive.napply(tableName, FUN, ...,forcedRef=TRUE)
rhive.sapply(tableName, FUN, ..., forcedRef=TRUE)
rhive.mrapply(tableName, mapperFUN, reducerFUN, mapInput=NULL,
  mapOutput=NULL, by=NULL, reduceInput=NULL,reduceOutput=NULL,
  mapperArgs=NULL, reducerArgs=NULL, bufferSize=-1L, verbose=FALSE, 
  forcedRef=TRUE)
rhive.mapapply(tableName, mapperFUN, mapInput=NULL, mapOutput=NULL,
  by=NULL, args=NULL, bufferSize=-1L, verbose=FALSE, forcedRef=TRUE)
rhive.reduceapply(tableName, reducerFUN, reduceInput=NULL, 
  reduceOutput=NULL, args=NULL, bufferSize=-1L, verbose=FALSE,
  forcedRef=TRUE)
}
\description{
R Distributed apply function using HQL
}
\arguments{
\item{tableName}{hive table name.}
\item{FUN}{the function to be applied.}
\item{...}{optional arguments to 'FUN'.}
\item{mapperFUN}{a function which is executed on each worker node.
  The so-called mapper typically maps input key/value pairs to 
  a set of intermediate key/value pairs.}
\item{reducerFUN}{a function which is executed on each worker node. 
  The so-called reducer reduces a set of intermediate values which share a
  key to a smaller set of values. If no reducer is used leave NULL.}
\item{mapInput}{map-input column list.}
\item{mapOutput}{map-output column list.}
\item{by}{cluster key column}
\item{reduceInput}{reduce-input column list.}
\item{reduceOutput}{reduce-output column list.}
\item{bufferSize}{streaming buffer size.}
\item{verbose}{print generated HQL.}
\item{args}{custom environment.}
\item{mapperArgs}{mapper custom environment.}
\item{reducerArgs}{reducer custom environment.}
\item{forcedRef}{the option which forces to create temp-table for result.}
} 
\author{
\email{rhive@nexr.com}
}
\examples{
## try to connect hive server
\dontrun{rhive.connect("hive-server-ip")}

## invoke napply for numeric return type
\dontrun{rhive.napply('emp', function(item) {
item * 10
},'sal')}

## invoke sapply for string return type
\dontrun{rhive.napply('emp', function(item) {
paste('NAME : ', item, sep='')
}, 'ename')}

## custom map/reduce script
\dontrun{map <- function(k, v) {
    if(is.null(v)) {
        put(NA, 1)
    }
    lapply(v, function(vv) {
        lapply(strsplit(x = vv, split = "\t")[[1]], 
            function(w) put(paste(args, w, sep = ""), 1))
    })
}
 
reduce <- function(k, vv) {
    put(k, sum(as.numeric(vv)))
}
 
rhive.mrapply("emp", map, reduce, c("ename", "position"), c("position", "one"),
    by="position", c("position", "one"), c("position", "count"))}


## close connection
\dontrun{rhive.close()}
}
\keyword{programming}
