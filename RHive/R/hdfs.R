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

rhive.hdfs.connect <- function(hdfsurl="hdfs://127.0.0.1:8020") {

     config <- .jnew("org/apache/hadoop/conf/Configuration")
     config$set(.jnew("java/lang/String","fs.default.name"),.jnew("java/lang/String",hdfsurl))
     
     fileSystem <- J("org.apache.hadoop.fs.FileSystem")
     
     fs <- fileSystem$get(config)
     
     assign('hdfsclient',fs,env=RHive:::.rhiveEnv)
   
   	 if(rhive.hdfs.exists('/rhive/lib/rhive_udf.jar'))
   	 	rhive.hdfs.rm('/rhive/lib/rhive_udf.jar')
   	 	
   	 result <- try(rhive.hdfs.put(paste(system.file(package="RHive"),"java","rhive_udf.jar",sep=.Platform$file.sep),'/rhive/lib/rhive_udf.jar'), silent = FALSE)
	 if(class(result) == "try-error") {
	 	sprintf("fail to connect HDFS with %s - %s",hdfsurl,result)
	 	return(NULL)
	 }
	 
	 return(fs)
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

rhive.save <- function(..., file, envir = parent.frame(), fileSystem = rhive.hdfs.defaults('hdfsclient')) {

	tmpfile <- paste("_rhive_save_",as.integer(Sys.time()),sep="")
	
	save(...,file=tmpfile, envir = envir)

	rhive.hdfs.put(tmpfile, file, fileSystem = fileSystem);
	
	unlink(tmpfile)
	
	TRUE
}

rhive.load <- function(file, envir = parent.frame(), fileSystem = rhive.hdfs.defaults('hdfsclient')) {

    tmpfile <- paste("_rhive_load_",as.integer(Sys.time()),sep="")

    rhive.hdfs.get(file, tmpfile, fileSystem = fileSystem);
	
	load(file=tmpfile, envir = envir)
	
	unlink(tmpfile)
	
	TRUE
}



rhive.hdfs.put <- function(source, target, sourcedelete = FALSE, overwrite = FALSE, fileSystem = rhive.hdfs.defaults('hdfsclient')) {

	sPath <- .jnew("org/apache/hadoop/fs/Path",.jnew("java/lang/String",source))
	tPath <- .jnew("org/apache/hadoop/fs/Path",.jnew("java/lang/String",paste('hdfs://',target,sep='')))

	result <- try(fileSystem$copyFromLocalFile(sourcedelete, overwrite, sPath,tPath), silent = FALSE)
	
	if(class(result) == "try-error") return(FALSE)
	
	TRUE
}

rhive.hdfs.get <- function(source, target, sourcedelete = FALSE, fileSystem = rhive.hdfs.defaults('hdfsclient')) {

	tPath <- .jnew("org/apache/hadoop/fs/Path",.jnew("java/lang/String",target))
	sPath <- .jnew("org/apache/hadoop/fs/Path",.jnew("java/lang/String",paste('hdfs://',source,sep='')))

	result <- try(fileSystem$copyToLocalFile(sourcedelete, sPath,tPath), silent = FALSE)
	
	if(class(result) == "try-error") return(FALSE)

	TRUE
}

rhive.hdfs.rm <- function(..., fileSystem = rhive.hdfs.defaults('hdfsclient')) {

	for(target in c(...)) {

		tPath <- .jnew("org/apache/hadoop/fs/Path",.jnew("java/lang/String",target))
	
		result <- try(fileSystem$delete(tPath), silent = FALSE)
		if(class(result) == "try-error") return(FALSE)
		
		#fileSystem$deleteOnExit(tPath)
	}
	
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


rhive.write.table <- function(dat, tablename = NULL, sep = ",", nastring = NULL, fileSystem = rhive.hdfs.defaults('hdfsclient'),hiveclient=rhive.defaults('hiveclient')) {

	if(missing(dat))
		stop("missing parameter")
	if(!is.data.frame(dat))
		stop("should be a data frame")
	if (is.null(tablename)) {
        if (length(substitute(dat)) == 1) 
            tablename <- as.character(substitute(dat))
        else 
        	tablename <- as.character(substitute(dat)[[2L]])
    }
    if (length(tablename) != 1L) 
        stop(sQuote(tablename), " should be a name")
	exportname <- tablename
	if(is.null(tablename)) {
		exportname <- paste("rhive_r_table",as.integer(Sys.time()),".rhive",sep="")
	}

	rowname <- "rowname"
    dat <- cbind(row.names(dat), dat)
    names(dat)[1L] <- rowname
	
	types <- sapply(dat, typeof)
	facs <- sapply(dat, is.factor)
	isreal <- (types == "double")
	isint <- (types == "integer") & !facs
	islogi <- (types == "logical")
	
	colspecs <- rep("STRING", length(dat))
	colspecs[isreal] <- "DOUBLE"
	colspecs[isint] <- "INT"
	colspecs[islogi] <- "BOOLEAN"
	
	names(colspecs) <- names(dat)

	hdfs_root_path <- paste("/rhive/data/",as.integer(Sys.time()),sep="")
    hdfs_path <- paste(hdfs_root_path,"/",exportname,sep="")
	
	hquery <- .generateCreateQuery(tablename, colspecs, hdfs_path = hdfs_root_path, sep = sep)
	
	dat[is.na(dat)] <- if(is.null(nastring)) "NULL" else nastring[1L]
	
	write.table(dat,file=exportname,quote=FALSE,row.names = FALSE, col.names=FALSE, sep = sep)
	rhive.hdfs.put(exportname, hdfs_path, sourcedelete = TRUE, overwrite = TRUE, fileSystem = fileSystem);
	
	client <- .jcast(hiveclient[[1]], new.class="org/apache/hadoop/hive/service/HiveClient",check = FALSE, convert.array = FALSE)
    client$execute(.jnew("java/lang/String",hquery))
	
	return(tablename)
}


rhive.script.export <- function(exportname, mapper = NULL, reducer = NULL, mapper_args=NULL, reducer_args=NULL, buffersize=-1L, fileSystem = rhive.hdfs.defaults('hdfsclient')) {

	
	if(!is.null(mapper)) {
		mapScript <- paste(system.file(package="RHive"),"resource","_mapper.template",sep=.Platform$file.sep)
		mtmpfile <- paste("_rhive_mapper_",as.integer(Sys.time()),sep="")
		
		.generateScript(mapper, mtmpfile, mapScript, "map", mapper_args, buffersize)
		
		rhive.hdfs.put(mtmpfile, paste("/rhive/script/",exportname,".mapper",sep=""), sourcedelete = TRUE, overwrite = TRUE, fileSystem = fileSystem);
	
	   #unlink(rtmpfile)
	}
	
	if(!is.null(reducer)) {
	
		reduceScript <- paste(system.file(package="RHive"),"resource","_reducer.template",sep=.Platform$file.sep)
		rtmpfile <- paste("_rhive_mapper_",as.integer(Sys.time()),sep="")
		
		.generateScript(reducer, rtmpfile, reduceScript,"reduce", reducer_args, buffersize)
		
		rhive.hdfs.put(rtmpfile, paste("/rhive/script/",exportname,".reducer",sep=""), sourcedelete = TRUE, overwrite = TRUE, fileSystem = fileSystem);
		
		#unlink(rtmpfile)
		
	}
	
}

rhive.script.unexport <- function(exportname,fileSystem = rhive.hdfs.defaults('hdfsclient')) {

	mapScript <- paste("/rhive/script/",exportname,".mapper",sep="")
	reduceScript <- paste("/rhive/script/",exportname,".reducer",sep="")
	
	if(rhive.hdfs.exists(mapScript,fileSystem=fileSystem)) {
		rhive.hdfs.rm(mapScript)
	}
	
	if(rhive.hdfs.exists(reduceScript,fileSystem=fileSystem)) {
		rhive.hdfs.rm(reduceScript)
	}
	
	return(TRUE)
}

.generateCreateQuery <- function (tablename, colspecs, hdfs_path, sep = ",")
{
    create <- paste("CREATE TABLE ", tablename , " (")
    
    colnames <- gsub("[^[:alnum:]_]+", "", names(colspecs))
    entries <- paste(colnames, colspecs)
    create <- paste(create, paste(entries, collapse = ", "), sep="")
    create <- paste(create, ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '", sep , "'",sep="")
    create <- paste(create, " STORED AS TEXTFILE LOCATION '", hdfs_path ,"'", sep="")
    
    create
}

.generateScript <- function(x, output, script, name, args, buffersize) {
	#custom_function <- paste(deparse(functionBody(x)),collapse="\n")
	custom_function <- paste(deparse(x),collapse="\n")
	
	prefix <- "#!/usr/bin/env Rscript\n"
	buffer <- sprintf("buffersize <- %s\n",buffersize)
	
	if(is.null(args)) 
		args <- ""
	user_args <- sprintf("args <- '%s'\n",as.character(args))
	fname <- sprintf("%s <- ",name)
	
	cat(sprintf("%s%s%s%s%s", prefix, buffer, user_args, fname, custom_function), file = output, sep="\n", append = FALSE)
	
	lines <- readLines(script)
	
	for(line in lines) {
		cat(sprintf("%s", line), file = output, sep="\n", append = TRUE)
	}
	
	status <- system(sprintf("chmod 775 %s", script), ignore.stderr = TRUE)
	
	if(status) {
		warning("no executable found")
		invisible(FALSE)
	}
	invisible(script)
}

.local_cleanup <- function(files){
  if(all(file.exists(files)))
    unlink(files)
}