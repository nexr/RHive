\name{rhive.basic}
\alias{rhive.basic}
\alias{rhive.basic.mode}
\alias{rhive.basic.range}
\alias{rhive.basic.merge}
\alias{rhive.basic.xtabs}
\alias{rhive.basic.cut}
\alias{rhive.basic.cut2}
\alias{rhive.basic.by}
\alias{rhive.basic.scale}
\alias{rhive.basic.t.test}
\alias{rhive.block.sample}
\title{R Distributed basic statistic function using Hive}
\usage{
rhive.basic.mode(tablename, col, forcedRef = TRUE)
rhive.basic.range(tablename, col)
rhive.basic.merge(x, y, by.x, by.y, forcedRef = TRUE)
rhive.basic.xtabs(formula, tablename)
rhive.basic.cut(tablename, col, breaks, right=TRUE, summary = FALSE, forcedRef = TRUE)
rhive.basic.cut2(tablename, col1, col2, breaks1, breaks2, right=TRUE, keepCol = FALSE, forcedRef = TRUE)
rhive.basic.by(tablename, INDICES, fun, arguments, forcedRef = TRUE)
rhive.basic.scale(tablename, col)
rhive.basic.t.test(x,col1,y,col2)
rhive.block.sample(tablename, percent = 0.01, seed = 0, subset) 
}
\description{
R Distributed basic statistic function using Hive
}
\arguments{
\item{tablename}{hive table name.}
\item{x, y}{table-names to be coerced to one or an object which can be coerced.}
\item{by.x, by.y}{specifications of the common columns.}
\item{col}{column name}
\item{col1}{column name}
\item{col2}{column name}
\item{formula}{a formula object with the cross-classifying variables
		  (separated by '+') on the right hand side (or an object
		  which can be coerced to a formula).}
\item{breaks}{a numeric vector of two or more cut points. a format is 'min:max:step' and 'step' is optional. or
either a numeric vector of two or more cut points or a single number (greater than or equal to 2) 
giving the number of intervals into which 'x' is to be cut.}
\item{breaks1}{a breaks of col1}
\item{breaks2}{a breaks of col2}
\item{summary}{a option whether summarize the result of cut or not.}
\item{INDICES}{a list of column to be grouped.}
\item{fun}{a hive function name to be applied.}
\item{arguments}{input data for a function. for examples, arguments=c("sal","deptno",3.2,"'NexR'")}
\item{right}{logical, indicating if the intervals should be closed on the right (and open on the left) or vice versa.}
\item{keepCol}{an option which keeps original columns}
\item{forcedRef}{the option which forces to create temp-table for result.}
\item{percent}{percent of data size which is picked up.}
\item{seed}{first selected block index.}
\item{subset}{an optional record-set specifying a subset of observations to be used.}
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

## divides the range of a column into intervals
\dontrun{rhive.basic.cut2('emp','dept','sal','loc',breaks1='0:5000:100',breaks2='0:100:10')}

## extract the summation of salary by group 
\dontrun{rhive.basic.by('emp','deptno','sum',c("sal"))}

## centers and/or scales the columns of table
\dontrun{rhive.basic.scale('emp','sal')}

## analyze two dataset 
\dontrun{rhive.basic.t.test(emp$sal,emp$age)}

## sampling
\dontrun{rhive.basic.sample("emp",subset="id < 100")}

## close connection
\dontrun{rhive.close()}
}
\keyword{programming}