\name{rhive.basic}
\alias{rhive.basic.mode}
\alias{rhive.basic.range}
\alias{rhive.basic.merge}
\alias{rhive.basic.xtabs}
\alias{rhive.basic.cut}
\alias{rhive.basic.by}
\alias{rhive.basic.scale}
\title{R Distributed basic statistic function using Hive}
\usage{
rhive.basic.mode(tablename, col)
rhive.basic.range(tablename, col)
rhive.basic.merge(x, y, by.x, by.y)
rhive.basic.xtabs(x, cols, tablename)
rhive.basic.cut(tablename, col, breaks, right=TRUE, summary = FALSE)
rhive.basic.by(tablename, INDICES, fun, arguments)
rhive.basic.scale(tablename, col)
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
\item{breaks}{a numeric vector of two or more cut points. a format is 'min:max:step' and 'step' is optional. or
either a numeric vector of two or more cut points or a single number (greater than or equal to 2) 
giving the number of intervals into which 'x' is to be cut.}
\item{summary}{a option whether summarize the result of cut or not.}
\item{INDICES}{a list of column to be grouped.}
\item{fun}{a hive function name to be applied.}
\item{arguments}{input data for a function. for examples, arguments=c("sal","deptno",3.2,"'NexR'")}
\item{right}{logical, indicating if the intervals should be closed on the right (and open on the left) or vice versa.}
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
\dontrun{rhive.basic.by('emp','deptno','sum',c("sal"))}

## centers and/or scales the columns of table
\dontrun{rhive.basic.scale('emp','sal')}

## close connection
\dontrun{rhive.close()}
}
\keyword{programming}