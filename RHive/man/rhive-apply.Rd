\name{rhive-aggregate}
\alias{rhive.aggregate}
\title{R Distributed apply function using HQL}
\usage{
rhive.napply(tablename, FUN, ...,hiveclient =rhive.defaults('hiveclient'))
rhive.sapply(tablename, FUN, ..., hiveclient =rhive.defaults('hiveclient'))
}
\description{
R Distributed apply function using HQL
}
\arguments{
\item{hiveclient}{a client for hive.}
\item{tablename}{hive table name.}
\item{FUN}{the function to be applied.}
\item{...}{optional arguments to 'FUN'.}
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
