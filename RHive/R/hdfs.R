# Copyright 2011 NexR
#    
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

rhive.hdfs.connect <- function(host="127.0.0.1", port=5003) {

     config <- .jnew("org/apache/hadoop/conf/Configuration")
     hdfsurl <- paste('hdfs://',host,':',port,sep='')
     config$set(.jnew("java/lang/String","fs.default.name"),.jnew("java/lang/String",hdfsurl))
     
     fileSystem <- J("org.apache.hadoop.fs.FileSystem")
     
     fs <- fileSystem$get(config)
     
     assign('hdfsclient',fs,env=RHive:::.rhiveEnv)
   
   	 if(rhive.hdfs.exists('/rhive/lib/rhive_udf.jar'))
   	 	rhive.hdfs.rm('/rhive/lib/rhive_udf.jar')
   	 rhive.hdfs.put(paste(system.file(package="RHive"),"java","rhive_udf.jar",sep=.Platform$file.sep),'/rhive/lib/rhive_udf.jar')
}

rhive.hdfs.defaults <- function(arg){
  if(missing(arg)){
    as.list(.rhiveEnv)
  } else { 
  	RHive:::.rhiveEnv[[arg]]
  }
}

rhive.hdfs.assign <- function(name, value) {

	result <- try(assign(name,value,envir=.rhiveExportEnv), silent = FALSE)
	if(class(result) == "try-error") return(FALSE)
	
	return(TRUE)

}

rhive.hdfs.ls <- function(path="/", fileSystem = rhive.hdfs.defaults('hdfsclient')) {

	rdata <- list()

    transformer <- J("com.nexr.rhive.util.TransformUtils")

	listStatus <- fileSystem$listStatus(.jnew("org/apache/hadoop/fs/Path",.jnew("java/lang/String",path)));

	for(index in c(1:listStatus$length)) {
	 	item <- .jcast(listStatus[[index]], new.class="org/apache/hadoop/fs/FileStatus",check = FALSE, convert.array = FALSE)
	 
	    item$getAccessTime()
	    item$getBlockSize()
	    item$getReplication()
	    item$isDir()
	
		splits <- transformer$tranform(item)
	
	    if(index == 1) {
		    rdata[[1]] <- c(splits[1])
		    rdata[[2]] <- c(splits[2])
		    rdata[[3]] <- c(splits[3])
		    rdata[[4]] <- c(splits[4])
		    rdata[[5]] <- c(splits[5])
		    rdata[[6]] <- c(splits[6])		    
	    }else {
		    rdata[[1]] <- c(rdata[[1]],splits[1])
		    rdata[[2]] <- c(rdata[[2]],splits[2])
		    rdata[[3]] <- c(rdata[[3]],splits[3])
		    rdata[[4]] <- c(rdata[[4]],splits[4])
		    rdata[[5]] <- c(rdata[[5]],splits[5])
		    rdata[[6]] <- c(rdata[[6]],splits[6])	
	    }
	    
	 }

	if(listStatus$length == 0) return(NULL)

    df <- as.data.frame(rdata)

	rownames(df) <- NULL
    colnames(df) <- c("permission", "owner", "group", "length", "modify-time", "file")

	return(df)
}

rhive.hdfs.put <- function(source, target, fileSystem = rhive.hdfs.defaults('hdfsclient')) {

	sPath <- .jnew("org/apache/hadoop/fs/Path",.jnew("java/lang/String",source))
	tPath <- .jnew("org/apache/hadoop/fs/Path",.jnew("java/lang/String",paste('hdfs://',target,sep='')))

	fileSystem$copyFromLocalFile(sPath,tPath)
	
	TRUE
}

rhive.hdfs.get <- function(source, target, fileSystem = rhive.hdfs.defaults('hdfsclient')) {

	tPath <- .jnew("org/apache/hadoop/fs/Path",.jnew("java/lang/String",target))
	sPath <- .jnew("org/apache/hadoop/fs/Path",.jnew("java/lang/String",paste('hdfs://',source,sep='')))

	fileSystem$copyToLocalFile(sPath,tPath)

	TRUE
}

rhive.hdfs.rm <- function(target, fileSystem = rhive.hdfs.defaults('hdfsclient')) {

	tPath <- .jnew("org/apache/hadoop/fs/Path",.jnew("java/lang/String",target))
	
	fileSystem$delete(tPath)
	#fileSystem$deleteOnExit(tPath)
	
	TRUE
}

rhive.hdfs.rename <- function(source, target, fileSystem = rhive.hdfs.defaults('hdfsclient')) {

	tPath <- .jnew("org/apache/hadoop/fs/Path",.jnew("java/lang/String",target))
	sPath <- .jnew("org/apache/hadoop/fs/Path",.jnew("java/lang/String",paste('hdfs://',source,sep='')))
	
	fileSystem$rename(sPath,tPath)
	
	TRUE
}

rhive.hdfs.exists <- function(path, fileSystem = rhive.hdfs.defaults('hdfsclient')) {

	tPath <- .jnew("org/apache/hadoop/fs/Path",.jnew("java/lang/String",path))
	
	return(fileSystem$exists(tPath))

}

rhive.hdfs.mkdirs <- function(path, fileSystem = rhive.hdfs.defaults('hdfsclient')) {

	tPath <- .jnew("org/apache/hadoop/fs/Path",.jnew("java/lang/String",path))
	
	fileSystem$mkdirs(tPath)
	
	TRUE
}

rhive.hdfs.close <- function(fileSystem = rhive.hdfs.defaults('hdfsclient')) {

	fileSystem$close()
	
	TRUE
}