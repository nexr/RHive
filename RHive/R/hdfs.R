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

rhive.hdfs.connect <- function(hdfsurl=rhive.hdfs.default.name()) {

	 if(is.null(hdfsurl))
	 	stop("missing parameter or HADOOP_HOME must be set")

     config <- .jnew("org/apache/hadoop/conf/Configuration")
     config$set(.jnew("java/lang/String","fs.default.name"),.jnew("java/lang/String",hdfsurl))
     
     fileSystem <- J("org.apache.hadoop.fs.FileSystem")
     
     fs <- fileSystem$get(config)
     fsshell <- .jnew("org/apache/hadoop/fs/FsShell",config)
     dfutils <- .jnew("com/nexr/rhive/util/DFUtils",config)
     
     hdfs <- list(fs,fsshell,dfutils)
     assign('hdfs',hdfs,envir=RHive:::.rhiveEnv)
   
   	 if(rhive.hdfs.exists('/rhive/lib/rhive_udf.jar'))
   	 	rhive.hdfs.rm('/rhive/lib/rhive_udf.jar')
   	 	
   	 result <- try(rhive.hdfs.put(paste(system.file(package="RHive"),"java","rhive_udf.jar",sep=.Platform$file.sep),'/rhive/lib/rhive_udf.jar'), silent = FALSE)
	 if(class(result) == "try-error") {
	 	sprintf("fail to connect HDFS with %s - %s",hdfsurl,result)
	 	return(NULL)
	 }
	 
	 return(hdfs)
}


rhive.hdfs.default.name <- function() {

	hdfsurl <- NULL
	config <- rhive.hdfs.defaults('hconfig')
    if(!is.null(config)) {
     	hdfsurl <- config$get("fs.default.name")
    }
    
    return(hdfsurl)

}

rhive.hdfs.defaults <- function(arg){
  if(missing(arg)){
    as.list(.rhiveEnv)
  } else { 
  	RHive:::.rhiveEnv[[arg]]
  }
}

.checkHDFSConnection <- function(hdfs=rhive.hdfs.defaults('hdfs')) {

	if(is.null(hdfs))
		stop("disconnected with HDFS. try to command 'rhive.hdfs.connect()'")

}

rhive.hdfs.ls <- function(path="/", hdfs = rhive.hdfs.defaults('hdfs')) {

    .checkHDFSConnection(hdfs)
    
    fileSystem <- .jcast(hdfs[[1]], new.class="org/apache/hadoop/fs/FileSystem",check = FALSE, convert.array = FALSE)

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

rhive.hdfs.du <- function(path="/", summary = FALSE, hdfs = rhive.hdfs.defaults('hdfs')) {

	.checkHDFSConnection(hdfs)
    fileSystem <- .jcast(hdfs[[1]], new.class="org/apache/hadoop/fs/FileSystem",check = FALSE, convert.array = FALSE)

	rdata <- list()
    transformer <- J("com.nexr.rhive.util.TransformUtils")

    if(summary) {
		listStatus <- fileSystem$globStatus(.jnew("org/apache/hadoop/fs/Path",.jnew("java/lang/String",path)));    
    }else {
		listStatus <- fileSystem$listStatus(.jnew("org/apache/hadoop/fs/Path",.jnew("java/lang/String",path)));
	}

	for(index in c(1:listStatus$length)) {
	 	item <- .jcast(listStatus[[index]], new.class="org/apache/hadoop/fs/FileStatus",check = FALSE, convert.array = FALSE)
	 	
	 	splits <- transformer$tranform(item)
	 	
	    if(index == 1) {	 
		    if(item$isDir()) {
		 		len <- fileSystem$getContentSummary(item$getPath())$getLength()
		 		rdata[[1]] <- c(len)
			    rdata[[2]] <- c(splits[6])	
		 	}else {
		 		rdata[[1]] <- c(splits[4])
			    rdata[[2]] <- c(splits[6])	
		 	}   
	    }else {	
		    if(item$isDir()) {
		 		len <- fileSystem$getContentSummary(item$getPath())$getLength()
		 		rdata[[1]] <- c(rdata[[1]],len)
			    rdata[[2]] <- c(rdata[[2]],splits[6])
		 	
		 	}else {
			    rdata[[1]] <- c(rdata[[1]],splits[4])
			    rdata[[2]] <- c(rdata[[2]],splits[6])	
		 	}
	    }
	}
	
	if(listStatus$length == 0) return(NULL)

    df <- as.data.frame(rdata)

	rownames(df) <- NULL
    colnames(df) <- c("length", "file")

	return(df)
}

rhive.save <- function(..., file, envir = parent.frame(), hdfs = rhive.hdfs.defaults('hdfs')) {

	tmpfile <- paste("_rhive_save_",as.integer(Sys.time()),sep="")
	
	save(...,file=tmpfile, envir = envir)

	result <- try(rhive.hdfs.put(tmpfile, file, hdfs = hdfs), silent = FALSE)
	
	if(class(result) == "try-error") {
	    unlink(tmpfile)
		return(FALSE)
	}
	
	unlink(tmpfile)
	TRUE
}

rhive.load <- function(file, envir = parent.frame(), hdfs = rhive.hdfs.defaults('hdfs')) {

    tmpfile <- paste("_rhive_load_",as.integer(Sys.time()),sep="")

    rhive.hdfs.get(file, tmpfile, hdfs = hdfs);
	
	result <- try(load(file=tmpfile, envir = envir), silent = FALSE)
	
	if(class(result) == "try-error") {
	    unlink(tmpfile)
		return(FALSE)
	}
	
	unlink(tmpfile)
	TRUE
}



rhive.hdfs.put <- function(source, target, sourcedelete = FALSE, overwrite = FALSE, hdfs = rhive.hdfs.defaults('hdfs')) {

    .checkHDFSConnection(hdfs)
    
    fileSystem <- .jcast(hdfs[[1]], new.class="org/apache/hadoop/fs/FileSystem",check = FALSE, convert.array = FALSE)

	sPath <- .jnew("org/apache/hadoop/fs/Path",.jnew("java/lang/String",source))
	tPath <- .jnew("org/apache/hadoop/fs/Path",.jnew("java/lang/String",paste('hdfs://',target,sep='')))

	result <- try(fileSystem$copyFromLocalFile(sourcedelete, overwrite, sPath,tPath), silent = FALSE)
	
	if(class(result) == "try-error") return(FALSE)
	
	TRUE
}

rhive.hdfs.get <- function(source, target, sourcedelete = FALSE, hdfs = rhive.hdfs.defaults('hdfs')) {

    .checkHDFSConnection(hdfs)
    
    fileSystem <- .jcast(hdfs[[1]], new.class="org/apache/hadoop/fs/FileSystem",check = FALSE, convert.array = FALSE)

	tPath <- .jnew("org/apache/hadoop/fs/Path",.jnew("java/lang/String",target))
	sPath <- .jnew("org/apache/hadoop/fs/Path",.jnew("java/lang/String",paste('hdfs://',source,sep='')))

	result <- try(fileSystem$copyToLocalFile(sourcedelete, sPath,tPath), silent = FALSE)
	
	if(class(result) == "try-error") return(FALSE)

	TRUE
}

rhive.hdfs.rm <- function(..., hdfs = rhive.hdfs.defaults('hdfs')) {

    .checkHDFSConnection(hdfs)
    
    fileSystem <- .jcast(hdfs[[1]], new.class="org/apache/hadoop/fs/FileSystem",check = FALSE, convert.array = FALSE)

	for(target in c(...)) {

		tPath <- .jnew("org/apache/hadoop/fs/Path",.jnew("java/lang/String",target))
	
		result <- try(fileSystem$delete(tPath), silent = FALSE)
		if(class(result) == "try-error") return(FALSE)
		
		#fileSystem$deleteOnExit(tPath)
	}
	
	TRUE
}

rhive.hdfs.rename <- function(source, target, hdfs = rhive.hdfs.defaults('hdfs')) {

    .checkHDFSConnection(hdfs)
    
    fileSystem <- .jcast(hdfs[[1]], new.class="org/apache/hadoop/fs/FileSystem",check = FALSE, convert.array = FALSE)

	tPath <- .jnew("org/apache/hadoop/fs/Path",.jnew("java/lang/String",target))
	sPath <- .jnew("org/apache/hadoop/fs/Path",.jnew("java/lang/String",paste('hdfs://',source,sep='')))
	
	fileSystem$rename(sPath,tPath)
	
	TRUE
}

rhive.hdfs.exists <- function(path, hdfs = rhive.hdfs.defaults('hdfs')) {

	if(missing(path))
		stop("missing argument")

    .checkHDFSConnection(hdfs)
    
    fileSystem <- .jcast(hdfs[[1]], new.class="org/apache/hadoop/fs/FileSystem",check = FALSE, convert.array = FALSE)

	tPath <- .jnew("org/apache/hadoop/fs/Path",.jnew("java/lang/String",path))
	
	return(fileSystem$exists(tPath))

}

rhive.hdfs.mkdirs <- function(path, hdfs = rhive.hdfs.defaults('hdfs')) {

	if(missing(path))
		stop("missing argument")

    .checkHDFSConnection(hdfs)
    
    fileSystem <- .jcast(hdfs[[1]], new.class="org/apache/hadoop/fs/FileSystem",check = FALSE, convert.array = FALSE)

	tPath <- .jnew("org/apache/hadoop/fs/Path",.jnew("java/lang/String",path))
	
	fileSystem$mkdirs(tPath)
	
	TRUE
}

rhive.hdfs.cat <- function(path, hdfs = rhive.hdfs.defaults('hdfs')) {

	if(missing(path))
		stop("missing argument")
		
	.checkHDFSConnection(hdfs)

    fsshell <- .jcast(hdfs[[2]], new.class="org/apache/hadoop/fs/FsShell",check = FALSE, convert.array = FALSE)

	fsshell$run(c("-cat",path))
	invisible()
}

rhive.hdfs.tail <- function(path, hdfs = rhive.hdfs.defaults('hdfs')) {

	if(missing(path))
		stop("missing argument")
		
	.checkHDFSConnection(hdfs)

    fsshell <- .jcast(hdfs[[2]], new.class="org/apache/hadoop/fs/FsShell",check = FALSE, convert.array = FALSE)

    fsshell$run(c("-tail",path))
	invisible()
}

rhive.hdfs.chmod <- function(cmd, path, recursive=FALSE, hdfs = rhive.hdfs.defaults('hdfs')) {

	if(missing(path))
		stop("missing argument")

	.checkHDFSConnection(hdfs)

	fsshell <- .jcast(hdfs[[2]], new.class="org/apache/hadoop/fs/FsShell",check = FALSE, convert.array = FALSE)
	
	if(recursive) {
		fsshell$run(c("-chmod","-R",cmd,path))
	}else {
		fsshell$run(c("-chmod",cmd,path))
	}
	invisible()
}


rhive.hdfs.chown <- function(cmd, path, recursive=FALSE, hdfs = rhive.hdfs.defaults('hdfs')) {

	if(missing(path))
		stop("missing argument")

	.checkHDFSConnection(hdfs)

	fsshell <- .jcast(hdfs[[2]], new.class="org/apache/hadoop/fs/FsShell",check = FALSE, convert.array = FALSE)
	
	if(recursive) {
		fsshell$run(c("-chown","-R",cmd,path))
	}else {
		fsshell$run(c("-chown",cmd,path))
	}
	invisible()

}

rhive.hdfs.chgrp <- function(cmd, path, recursive=FALSE, hdfs = rhive.hdfs.defaults('hdfs')) {

	if(missing(path))
		stop("missing argument")

	.checkHDFSConnection(hdfs)

	fsshell <- .jcast(hdfs[[2]], new.class="org/apache/hadoop/fs/FsShell",check = FALSE, convert.array = FALSE)
	
	if(recursive) {
		fsshell$run(c("-chgrp","-R",cmd,path))
	}else {
		fsshell$run(c("-chgrp",cmd,path))
	}
	invisible()

}


rhive.hdfs.close <- function(hdfs = rhive.hdfs.defaults('hdfs')) {

    .checkHDFSConnection(hdfs)
    
    fileSystem <- .jcast(hdfs[[1]], new.class="org/apache/hadoop/fs/FileSystem",check = FALSE, convert.array = FALSE)
	fileSystem$close()
	
    rm("hdfs",envir=.rhiveEnv)
	
	TRUE
}


rhive.write.table <- function(dat, tablename = NULL, sep = ",", nastring = NULL, hdfs = rhive.hdfs.defaults('hdfs'), hiveclient=rhive.hdfs.defaults('hiveclient')) {

    if(is.null(hiveclient))
	    stop("disconnected with hiveserver. try to command 'rhive.connect(hive-server-ip)'")

    .checkHDFSConnection(hdfs)
    
    fileSystem <- .jcast(hdfs[[1]], new.class="org/apache/hadoop/fs/FileSystem",check = FALSE, convert.array = FALSE)
        
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
        
	exportname <- paste(tablename,".rhive",sep="")
	
	if(is.null(tablename)) {
		stop("tablename should be named")
	}

	if(length(intersect(names(dat),"rhive_row")) == 0) {
		rowname <- "rhive_row"
	    dat <- cbind(row.names(dat), dat)
	    names(dat)[1L] <- rowname
    }
	
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

	dirname <- paste(tablename,"_",as.integer(Sys.time()),sep="")
	
	hdfs_root_path <- paste("/rhive/data/",dirname,sep="")
    hdfs_path <- paste(hdfs_root_path,"/",exportname,sep="")
	
	hquery <- .generateCreateQuery(tablename, colspecs, hdfs_path = hdfs_root_path, sep = sep)
	
	dat[is.na(dat)] <- if(is.null(nastring)) "NULL" else nastring[1L]
	
	if(rhive.exist.table(tablename)) {
		stop(paste(sQuote(tablename)," already exists. command 'rhive.query('DROP TABLE ",tablename,"')'",sep=""))
	}
	
	write.table(dat,file=exportname,quote=FALSE,row.names = FALSE, col.names=FALSE, sep = sep)
	rhive.hdfs.put(exportname, hdfs_path, sourcedelete = TRUE, overwrite = TRUE, hdfs = hdfs);
	
	client <- .jcast(hiveclient[[1]], new.class="org/apache/hadoop/hive/service/HiveClient",check = FALSE, convert.array = FALSE)
    client$execute(.jnew("java/lang/String",hquery))
	
	return(tablename)
}


rhive.script.export <- function(exportname, mapper = NULL, reducer = NULL, mapper_args=NULL, reducer_args=NULL, buffersize=-1L, hdfs = rhive.hdfs.defaults('hdfs')) {

    .checkHDFSConnection(hdfs)
    
    fileSystem <- .jcast(hdfs[[1]], new.class="org/apache/hadoop/fs/FileSystem",check = FALSE, convert.array = FALSE)
	
	map <- "NULL"
	reduce <- "NULL"
	
	if(!is.null(mapper)) {
		mapScript <- paste(system.file(package="RHive"),"resource","_mapper.template",sep=.Platform$file.sep)
		mtmpfile <- paste("_rhive_mapper_",as.integer(Sys.time()),sep="")
		
		.generateScript(mapper, mtmpfile, mapScript, "map", mapper_args, buffersize)
		
		rhive.hdfs.put(mtmpfile, paste("/rhive/script/",exportname,".mapper",sep=""), sourcedelete = TRUE, overwrite = TRUE, hdfs = hdfs);
	
	    map <- paste("/rhive/script/",exportname,".mapper",sep="")
	   #unlink(rtmpfile)
	}
	
	if(!is.null(reducer)) {
	
		reduceScript <- paste(system.file(package="RHive"),"resource","_reducer.template",sep=.Platform$file.sep)
		rtmpfile <- paste("_rhive_mapper_",as.integer(Sys.time()),sep="")
		
		.generateScript(reducer, rtmpfile, reduceScript,"reduce", reducer_args, buffersize)
		
		rhive.hdfs.put(rtmpfile, paste("/rhive/script/",exportname,".reducer",sep=""), sourcedelete = TRUE, overwrite = TRUE, hdfs = hdfs);
		
		reduce <- paste("/rhive/script/",exportname,".reducer",sep="")
		#unlink(rtmpfile)
		
	}
	
	mrscriptnames <- c(map,reduce)
	
	return(mrscriptnames)
	
}

rhive.script.unexport <- function(exportname,hdfs = rhive.hdfs.defaults('hdfs')) {

    .checkHDFSConnection(hdfs)

	mapScript <- paste("/rhive/script/",exportname,".mapper",sep="")
	reduceScript <- paste("/rhive/script/",exportname,".reducer",sep="")
	
	if(rhive.hdfs.exists(mapScript,hdfs=hdfs)) {
		rhive.hdfs.rm(mapScript)
	}
	
	if(rhive.hdfs.exists(reduceScript,hdfs=hdfs)) {
		rhive.hdfs.rm(reduceScript)
	}
	
	return(TRUE)
}


rhive.hdfs.info <- function(path, hdfs = rhive.hdfs.defaults('hdfs')) {

	dfutils <- .jcast(hdfs[[3]], new.class="com/nexr/rhive/util/DFUtils",check = FALSE, convert.array = FALSE)
	
	metas <- dfutils$getFileInfo(path)
	
	rdata <- list()

	rdata[[1]] <- c(as.numeric(strsplit(metas[1],' ')[[1]][1]))
	rdata[[2]] <- c(as.numeric(metas[2]))
	rdata[[3]] <- c(as.numeric(metas[3]))
	rdata[[4]] <- c(as.numeric(metas[4]))

    df <- as.data.frame(rdata)

	rownames(df) <- NULL
    colnames(df) <- c("size", "dirs", "files", "blocks")

	return(df)

}


.generateCreateQuery <- function (tablename, colspecs, hdfs_path, sep = ",")
{
    create <- paste("CREATE TABLE ", tablename , " (")
    
    colnames <- gsub("[^[:alnum:]_]+", "", names(colspecs))
    
    if(any(duplicated(tolower(colnames))) == TRUE) {
    	stop(paste("hive doesn't support case-sensitive column-name :",paste(colnames,collapse=",")))
    }

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
	
	if(status != 0) {
		warning("no executable found")
		invisible(FALSE)
	}
	invisible(script)
}

.local_cleanup <- function(files){
  if(all(file.exists(files)))
    unlink(files)
}