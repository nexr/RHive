\name{rhive-fn}
\alias{hiveConnect}
\alias{hiveQuery}
\alias{hiveExport}
\alias{hiveExportAll}
\alias{hiveClose}
\alias{hiveListTable}
\alias{hiveDescTable}
\alias{hiveLoadTable}
\title{Function style API}
\description{

}
\usage{
hiveConnect(host,port=10000) 
hiveClose(hiveclient=rhive.defaults('hiveclient'))
hiveQuery(query, fetchsize = 40, limit = -1, hiveclient=rhive.defaults('hiveclient'))
hiveExport(fn_name, hosts = "localhost", port = 6311, limit = 104857600)
hiveExportAll(name, hosts = "localhost", port = 6311, limit = 104857600)
hiveListTable(hiveclient=rhive.defaults('hiveclient'))
hiveDescTable(tablename,detail=FALSE,hiveclient=rhive.defaults('hiveclient'))
hiveLoadTable(tablename, limit = -1, hiveclient=rhive.defaults('hiveclient'))
}
\arguments{
\item{query}{hive query.}
\item{fetchsize}{fetch size for result.}
\item{limit}{total result size.}
\item{hiveclient}{a client for hive.}
} 
\author{
\email{rhive@nexr.com}
}
\details{

}
\value{

}
\references{

}
\note{

}
\keyword{programming}