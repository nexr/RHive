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
\alias{rhive.hdfs.close}
\title{R functions to communicate with HDFS}
\usage{
rhive.save(..., file, envir = parent.frame(), fileSystem = rhive.hdfs.defaults('hdfsclient'))
rhive.load(file, envir = parent.frame(), fileSystem = rhive.hdfs.defaults('hdfsclient'))
rhive.hdfs.connect(hdfs="127.0.0.1", hport=8020)
rhive.hdfs.ls(path="/", fileSystem = rhive.hdfs.defaults('hdfsclient'))
rhive.hdfs.get(source, target, sourcedelete = FALSE, fileSystem = rhive.hdfs.defaults('hdfsclient'))
rhive.hdfs.put(source, target, sourcedelete = FALSE, overwrite = FALSE, fileSystem = rhive.hdfs.defaults('hdfsclient'))
rhive.hdfs.rm(..., fileSystem = rhive.hdfs.defaults('hdfsclient'))
rhive.hdfs.rename(source, target, fileSystem = rhive.hdfs.defaults('hdfsclient'))
rhive.hdfs.exists(path, fileSystem = rhive.hdfs.defaults('hdfsclient'))
rhive.hdfs.mkdirs(path, fileSystem = rhive.hdfs.defaults('hdfsclient'))
rhive.hdfs.close(fileSystem = rhive.hdfs.defaults('hdfsclient'))
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

rhive.save : save R Objects to HDFS 

rhive.load : load R Objects from HDFS
}
\arguments{
\item{hdfs}{namenode address for connecting to hdfs.}
\item{source}{full path of source data.}
\item{target}{full path of target data.}
\item{file}{the full-name of the file where the data will be saved or loaded}
\item{path}{hdfs's full path.}
\item{hport}{namenode listen port.}
\item{fileSystem}{a client for hdfs.}
\item{envir}{environment to search for objects to be saved or loaded.}
\item{sourcedelete}{indicates if the source should be removed.}
\item{overwrite}{if path exists,this option indicates whether to overwrite.}
\item{...}{target path list.}
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
