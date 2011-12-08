\name{rhive-aggregate}
\alias{rhive.aggregate}
\alias{rhive.napply}
\alias{rhive.sapply}
\alias{rhive.mrapply}
\alias{rhive.mapapply}
\alias{rhive.reduceapply}
\title{R Distributed apply function using HQL}
\usage{
rhive.napply(tablename, FUN, ...,hiveclient =rhive.defaults('hiveclient'))
rhive.sapply(tablename, FUN, ..., hiveclient =rhive.defaults('hiveclient'))
rhive.mrapply(tablename, mapperFUN, reducerFUN, mapinput=NULL, mapoutput=NULL, by=NULL, reduceinput=NULL,reduceoutput=NULL, buffersize=-1L, verbose=FALSE, hiveclient =rhive.defaults('hiveclient'))
rhive.mapapply(tablename, mapperFUN, mapinput=NULL, mapoutput=NULL, by=NULL, buffersize=-1L, verbose=FALSE, hiveclient =rhive.defaults('hiveclient'))
rhive.reduceapply(tablename, reducerFUN, reduceinput=NULL,reduceoutput=NULL, buffersize=-1L, verbose=FALSE, hiveclient =rhive.defaults('hiveclient'))
}
\description{
R Distributed apply function using HQL
}
\arguments{
\item{hiveclient}{a client for hive.}
\item{tablename}{hive table name.}
\item{FUN}{the function to be applied.}
\item{...}{optional arguments to 'FUN'.}
\item{mapperFUN}{optional arguments to 'FUN'.}
\item{reducerFUN}{optional arguments to 'FUN'.}
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
