\name{rhive-apply}
\alias{rhive.napply}
\alias{rhive.sapply}
\alias{rhive.mrapply}
\alias{rhive.mapapply}
\alias{rhive.reduceapply}
\title{R Distributed apply function using HQL}
\usage{
rhive.napply(tablename, FUN, ...,hiveclient =rhive.defaults('hiveclient'))
rhive.sapply(tablename, FUN, ..., hiveclient =rhive.defaults('hiveclient'))
rhive.mrapply(tablename, mapperFUN, reducerFUN, mapinput=NULL, mapoutput=NULL, by=NULL, reduceinput=NULL,reduceoutput=NULL, mapper_args=NULL, reducer_args=NULL, buffersize=-1L, verbose=FALSE, hiveclient =rhive.defaults('hiveclient'))
rhive.mapapply(tablename, mapperFUN, mapinput=NULL, mapoutput=NULL, by=NULL, args=NULL, buffersize=-1L, verbose=FALSE, hiveclient =rhive.defaults('hiveclient'))
rhive.reduceapply(tablename, reducerFUN, reduceinput=NULL,reduceoutput=NULL, args=NULL, buffersize=-1L, verbose=FALSE, hiveclient =rhive.defaults('hiveclient'))
}
\description{
R Distributed apply function using HQL
}
\arguments{
\item{hiveclient}{a client for hive.}
\item{tablename}{hive table name.}
\item{FUN}{the function to be applied.}
\item{...}{optional arguments to 'FUN'.}
\item{mapperFUN}{a function which is executed on each worker node. 
	The so-called mapper typically maps input key/value pairs to 
	a set of intermediate key/value pairs.}
\item{reducerFUN}{a function which is executed on each worker node. 
	The so-called reducer reduces a set of intermediate values which share a
    key to a smaller set of values. If no reducer is used leave NULL.}
\item{mapinput}{map-input column list.}
\item{mapoutput}{map-output column list.}
\item{by}{cluster key column}
\item{reduceinput}{reduce-input column list.}
\item{reduceoutput}{reduce-output column list.}
\item{buffersize}{streaming buffer size.}
\item{verbose}{print generated HQL.}
\item{args}{custom environment.}
\item{mapper_args}{mapper custom environment.}
\item{reducer_args}{reducer custom environment.}
} 
\author{
\email{rhive@nexr.com}
}
\examples{
## try to connect hive server
\dontrun{rhive.connect("127.0.0.1")}

## invoke napply for numeric return type
\dontrun{rhive.napply('emp',function(item) {
item * 10
},'sal')}

## invoke sapply for string return type
\dontrun{rhive.napply('emp',function(item) {
paste('NAME : ',item,sep='')
},'ename')}

## close connection
\dontrun{rhive.close()}
}
\keyword{programming}
