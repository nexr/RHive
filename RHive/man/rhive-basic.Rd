\name{rhive.basic}
\alias{rhive.basic.mode}
\alias{rhive.basic.range}
\alias{rhive.basic.merge}
\alias{rhive.basic.xtabs}
\alias{rhive.basic.cut}
\alias{rhive.basic.by}
\title{R Distributed basic statistic function using Hive}
\usage{
rhive.basic.mode(tablename, col)
rhive.basic.range(tablename, col)
rhive.basic.merge(x, y, by.x, by.y)
rhive.basic.xtabs(x, cols, tablename)
rhive.basic.cut(tablename, col, breaks, summary = FALSE)
rhive.basic.by(tablename, cols, INDICES, FUN)
}
\description{
R Distributed basic statistic function using Hive
}
\arguments{
\item{tablename}{hive table name.}
\item{x, y}{table-names to be coerced to one or an object which can be coerced.}
\item{by.x, by.y}{specifications of the common columns.}
\item{col}{column name}
\item{cols}{column names or the cross-classifying variables.}
\item{breaks}{a numeric vector of two or more cut points. a format is 'min:max:step' and 'step' is optional.}
\item{summary}{a option whether summarize the result of cut or not.}
\item{INDICES}{a list of column to be grouped.}
\item{FUN}{a hive function name to be applied.}
} 
\author{
\email{rhive@nexr.com}
}
\examples{
## try to connect hive server
\dontrun{rhive.connect("hive-server-ip")}

## find the most frequency data of specified column
\dontrun{rhive.basic.mode('emp','deptno')}

## calculate min,max of specified column
\dontrun{rhive.basic.range('emp','sal')}

## merge two tables using shared column
\dontrun{rhive.basic.merge('emp','dept',by.x='deptno',by.y='id')}

DF <- as.data.frame(UCBAdmissions)

\dontrun{rhive.write.table(DF)}

## Nice for taking margins ...
\dontrun{rhive.basic.xtabs('freq',c('gender','admit'),'df')}

## divides the range of a column into intervals
\dontrun{rhive.basic.cut('emp','sal',breaks='0:5000:100')}

## extract the summation of salary by group 
\dontrun{rhive.basic.by('emp',c('sal')),'deptno','sum')}

## close connection
\dontrun{rhive.close()}
}
\keyword{programming}