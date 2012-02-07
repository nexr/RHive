\name{rhive-hdfs}
\alias{rhive.save}
\alias{rhive.load}
\alias{rhive.hdfs.connect}
\alias{rhive.hdfs.ls}
\alias{rhive.hdfs.get}
\alias{rhive.hdfs.put}
\alias{rhive.hdfs.rm}
\alias{rhive.hdfs.rename}
\alias{rhive.hdfs.exists}
\alias{rhive.hdfs.mkdirs}
\alias{rhive.hdfs.cat}
\alias{rhive.hdfs.tail}
\alias{rhive.hdfs.du}
\alias{rhive.hdfs.close}
\alias{rhive.write.table}
\alias{rhive.hdfs.info}
\alias{rhive.hdfs.chmod}
\alias{rhive.hdfs.chown}
\alias{rhive.hdfs.chgrp}
\title{R functions to communicate with HDFS}
\usage{
rhive.save(..., file, envir = parent.frame(), hdfs = rhive.hdfs.defaults('hdfs'))
rhive.load(file, envir = parent.frame(), hdfs = rhive.hdfs.defaults('hdfs'))
rhive.hdfs.connect(hdfsurl=rhive.hdfs.default.name())
rhive.hdfs.ls(path="/", hdfs = rhive.hdfs.defaults('hdfs'))
rhive.hdfs.get(source, target, sourcedelete = FALSE, hdfs = rhive.hdfs.defaults('hdfs'))
rhive.hdfs.put(source, target, sourcedelete = FALSE, overwrite = FALSE, hdfs = rhive.hdfs.defaults('hdfs'))
rhive.hdfs.rm(..., hdfs = rhive.hdfs.defaults('hdfs'))
rhive.hdfs.rename(source, target, hdfs = rhive.hdfs.defaults('hdfs'))
rhive.hdfs.exists(path, hdfs = rhive.hdfs.defaults('hdfs'))
rhive.hdfs.mkdirs(path, hdfs = rhive.hdfs.defaults('hdfs'))
rhive.hdfs.cat(path, hdfs = rhive.hdfs.defaults('hdfs')) 
rhive.hdfs.tail(path, hdfs = rhive.hdfs.defaults('hdfs')) 
rhive.hdfs.du(path="/", summary = FALSE, hdfs = rhive.hdfs.defaults('hdfs'))
rhive.hdfs.close(hdfs = rhive.hdfs.defaults('hdfs'))
rhive.write.table(dat, tablename = NULL, sep = ",", nastring = NULL, hdfs = rhive.hdfs.defaults('hdfs'), hiveclient=rhive.hdfs.defaults('hiveclient'))
rhive.hdfs.info(path, hdfs = rhive.hdfs.defaults('hdfs'))
rhive.hdfs.chmod(cmd, path, recursive=FALSE, hdfs = rhive.hdfs.defaults('hdfs'))
rhive.hdfs.chown(cmd, path, recursive=FALSE, hdfs = rhive.hdfs.defaults('hdfs'))
rhive.hdfs.chgrp(cmd, path, recursive=FALSE, hdfs = rhive.hdfs.defaults('hdfs'))
}
\description{
R functions to communicate with HDFS
}
\details{
rhive.hdfs.connect : Connect to HDFS

rhive.hdfs.ls : Lists the contents of the directory specified by path, 
showing the names, permissions, owner, size and modification date for each entry.

rhive.hdfs.put : Copy the file or directory from the local file system identified 
by source to target within the HDFS.

rhive.hdfs.get : Copy the file or directory in HDFS identified by source to 
the local file system path identified by target.

rhive.hdfs.rm : Removes the file or empty directory identified by path.

rhive.hdfs.rename : Rename the file or directory identified by source to target within the HDFS.

rhive.hdfs.exists : Check whether the file or directory specified by path is or not.

rhive.hdfs.mkdirs : Creates a directory named path in HDFS.

rhive.hdfs.close : Close hdfs connection

rhive.save : save R Objects to HDFS as R data format 

rhive.load : load R data format file stored in HDFS

rhive.write.table : create Hive table using R data.frame

rhive.hdfs.info : report block information of path

rhive.hdfs.chmod : change mode for specified path. 

rhive.hdfs.chown : change ownership for specified path.

rhive.hdfs.chgrp : change group for specified path.
}
\arguments{
\item{hdfsurl}{namenode url for connecting to hdfs.}
\item{source}{full path of source data.}
\item{target}{full path of target data.}
\item{file}{the full-name of the file where the data will be saved or loaded}
\item{path}{hdfs's full path.}
\item{hdfs}{a client for hdfs.}
\item{envir}{environment to search for objects to be saved or loaded.}
\item{sourcedelete}{indicates if the source should be removed.}
\item{overwrite}{if path exists,this option indicates whether to overwrite.}
\item{...}{target path list.}
\item{dat}{the object to be written, preferably a data frame.}
\item{tablename}{a character string naming a table}
\item{sep}{the field separator string.  Values within each row of 'dat'
          are separated by this string}
\item{nastring}{default value for NA.}
\item{hiveclient}{a client for hive.}
\item{summary}{summarize result of 'du'.}
\item{cmd}{specific command. chmod's command is 775 or chown's command is user-id.}
\item{recursive}{apply command recursively}
} 
\author{
\email{rhive@nexr.com}
}
\examples{
## try to connect hdfs namenode
\dontrun{rhive.hdfs.connect()}

## get list of specified path
\dontrun{rhive.hdfs.ls()}

## load local-file to hdfs
\dontrun{rhive.hdfs.put('/data/rhive.txt','/rhive/data/load.txt)}

## download data from hdfs to local-file
\dontrun{rhive.hdfs.get('/rhive/data/load.txt','/data/rhive.txt')}

## delete data in hdfs
\dontrun{rhive.hdfs.rm('/rhive/data/load.txt')}

## close connection
\dontrun{rhive.hdfs.close()}
}
\keyword{programming}
