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


rhive.init <- function(hive=NULL,libs=NULL,hadoop=NULL,hlibs=NULL,verbose=FALSE){

  if(is.null(hive)) hive <- Sys.getenv("HIVE_HOME")
  if(hive=="")
    stop(sprintf("HIVE_HOME(%s) is missing. Please set it and rerun", Sys.getenv("HIVE_HOME")))
  if(is.null(libs)) libs <- sprintf("%s/lib",hive)
  
  if(verbose) cat(sprintf("Detected hive=%s and libs=%s\n",hive,libs))
  
  if(is.null(hadoop)) hadoop <- Sys.getenv("HADOOP_HOME")
  if(hadoop=="") {
    print("HADOOP_HOME is missing. HDFS functions doesn't work")
    assign("slaves",c("127.0.0.1"),envir=.rhiveEnv)
  } else {  
	  if(is.null(hlibs)) hlibs <- sprintf("%s/lib",hadoop)
	  
	  slaves <- try(read.csv(sprintf("%s/conf/slaves",hadoop),header=FALSE)$V1,silent=TRUE)
	  if(class(slaves) != "try-error")
	  	assign("slaves",as.character(slaves),envir=.rhiveEnv)
  
  }
  
  if(hadoop=="")
    rhive.CP <- c(list.files(libs,full.names=TRUE,pattern="jar$",recursive=FALSE)
               ,list.files(paste(system.file(package="RHive"),"java",sep=.Platform$file.sep),pattern="jar$",full=T))
  else {
  	rhive.CP <- c(list.files(libs,full.names=TRUE,pattern="jar$",recursive=FALSE)
               ,list.files(paste(system.file(package="RHive"),"java",sep=.Platform$file.sep),pattern="jar$",full=T)
               ,list.files(hadoop,full.names=TRUE,pattern="jar$",recursive=FALSE)
               ,list.files(hlibs,full.names=TRUE,pattern="jar$",recursive=FALSE)
               ,sprintf("%s/conf",hadoop))
  }
  assign("classpath",rhive.CP,envir=.rhiveEnv)
  .jinit(classpath= rhive.CP)

  if(hadoop!="") {
	  config <- .jnew("org/apache/hadoop/conf/Configuration")
	  classloader <- .jclassLoader()
	  config$setClassLoader(classloader)
	  assign("hconfig",config,envir=.rhiveEnv)
  }
  
  options(show.error.messages = TRUE)
}

rhive.defaults <- function(arg){
  if(missing(arg)){
    as.list(.rhiveEnv)
  } else { 
  	RHive:::.rhiveEnv[[arg]]
  }
}

.checkConnection <- function(hiveclient=rhive.defaults('hiveclient')) {

	if(is.null(hiveclient))
		stop("disconnected with hiveserver. try to command 'rhive.connect(hive-server-ip)'")

}

rhive.assign <- function(name, value) {

	result <- try(assign(name,value,envir=.rhiveExportEnv), silent = FALSE)
	if(class(result) == "try-error") return(FALSE)
	
	return(TRUE)

}

rhive.rm <- function(name) {

	result <- try(rm(name,envir=.rhiveExportEnv), silent = FALSE)
	if(class(result) == "try-error") return(FALSE)
	
	return(TRUE)

}


rhive.env <- function(ALL=FALSE) {

	hadoop_home <- Sys.getenv("HADOOP_HOME")
	hive_home <- Sys.getenv("HIVE_HOME")

	slaves <- rhive.defaults('slaves')
	classpath <- rhive.defaults('classpath')
	rhiveclient <- rhive.defaults('hiveclient')
	
	cat(sprintf("Hive Home Directory : %s\n", hive_home))
	cat(sprintf("Hadoop Home Directory : %s\n", hadoop_home))
	
	if(!is.null(slaves)) {
		cat(sprintf("Default RServe List\n"))
		cat(sprintf("%s", unlist(slaves)))
		cat(sprintf("\n"))
	}else {
		cat(sprintf("No RServe\n"))
	}
	
	if(is.null(rhiveclient)) {
		cat(sprintf("Disconnected HiveServer and HDFS\n"))
	}else {
		if(!is.null(rhiveclient[[3]])) {
			cat(sprintf("Connected HiveServer : %s:%s\n", rhiveclient[[3]][1], rhiveclient[[3]][2]))
		}else {
			cat(sprintf("Disconnected HiveServer\n"))
		}
		if(!is.null(rhiveclient[[5]]))
			cat(sprintf("Connected HDFS : %s\n", rhiveclient[[6]]))
		else
			cat(sprintf("Disconnected HDFS\n"))
	}
		
	if(ALL) {
		cat(sprintf("RHive Library List\n"))
		cat(sprintf("%s",classpath))
		cat(sprintf("\n"))
	}

}


rhive.connect <- function(host="127.0.0.1",port=10000, hdfsurl=NULL ,hosts = rhive.defaults('slaves')) {

	 filesystem <- NULL

	 if(!is.null(hdfsurl)) {
     	filesystem <- rhive.hdfs.connect(hdfsurl)
     }else {
     	config <- rhive.defaults('hconfig')
     	if(!is.null(config)) {
     		hdfsurl <- config$get("fs.default.name")
     		hdfs <- rhive.hdfs.connect(hdfsurl)
     	}
     }

	 TSocket <- J("org.apache.thrift.transport.TSocket")
     TProtocol <- J("org.apache.thrift.protocol.TProtocol")
     HiveClient <- J("org.apache.hadoop.hive.service.HiveClient")
     hivecon <- .jnew("org/apache/thrift/transport/TSocket",.jnew("java/lang/String",host),as.integer(port))
     tpt <- .jnew("org/apache/thrift/protocol/TBinaryProtocol",.jcast(hivecon, new.class="org/apache/thrift/transport/TTransport",check = FALSE, convert.array = FALSE))
     client <- .jnew("org/apache/hadoop/hive/service/HiveClient",.jcast(tpt, new.class="org/apache/thrift/protocol/TProtocol",check = FALSE, convert.array = FALSE))
     
     result <- try(hivecon$open(), silent = FALSE)
     if(class(result) == "try-error") {
     	if(!is.null(hdfs)) {
     		rhive.hdfs.close(hdfs)
     	}
 		sprintf("fail to connect RHive [hiveserver = %s:%s, hdfs = %s]\n", host,port,hdfsurl)
 		return(NULL)
     }
     
     client$execute(.jnew("java/lang/String","add jar hdfs:///rhive/lib/rhive_udf.jar"))
     client$execute(.jnew("java/lang/String","create temporary function R as 'com.nexr.rhive.hive.udf.RUDF'"))
     client$execute(.jnew("java/lang/String","create temporary function RA as 'com.nexr.rhive.hive.udf.RUDAF'"))
     client$execute(.jnew("java/lang/String","create temporary function unfold as 'com.nexr.rhive.hive.udf.GenericUDTFUnFold'"))
     client$execute(.jnew("java/lang/String","create temporary function expand as 'com.nexr.rhive.hive.udf.GenericUDTFExpand'"))

     hiveclient <- list(client,hivecon,c(host,port),hosts,hdfs,hdfsurl)
     
     class(hiveclient) <- "rhive.client.connection"
     #reg.finalizer(hiveclient,function(r) {
     #     hivecon <- .jcast(r[[2]], new.class="org/apache/thrift/transport/TSocket",check = FALSE, convert.array = FALSE) 
     #     hivecon$close()	  
     #      print("call finalizer")
    # })
     assign('hiveclient',hiveclient,env=RHive:::.rhiveEnv)

}

rhive.close <- function(hiveclient=rhive.defaults('hiveclient')) {

    .checkConnection (hiveclient)

	hivecon <- .jcast(hiveclient[[2]], new.class="org/apache/thrift/transport/TSocket",check = FALSE, convert.array = FALSE)
	hivecon$close()

	if(!is.null(hiveclient[[5]])) {
		rhive.hdfs.close(hiveclient[[5]])
	}

	rm("hiveclient",envir=.rhiveEnv)

	return(TRUE)
	
}

rhive.query <- function(query, fetchsize = 40, limit = -1, hiveclient=rhive.defaults('hiveclient')) {

	.checkConnection(hiveclient)

	rdata <- list()

	client <- .jcast(hiveclient[[1]], new.class="org/apache/hadoop/hive/service/HiveClient",check = FALSE, convert.array = FALSE)
    client$execute(.jnew("java/lang/String",query))
     
    fullSchema <- client$getSchema();
    jfullSchema <- .jcast(fullSchema, new.class="org/apache/hadoop/hive/metastore/api/Schema",check = FALSE, convert.array = TRUE)
    fschema <- jfullSchema$getFieldSchemas();
    
    if(!is.null(fschema)) {
	    schema <- .jcast(fschema, new.class="java/util/List",check = FALSE, convert.array = FALSE)
	    if (!schema$isEmpty()) {
	      vlist <- c(0:(schema$size() - 1))
	      for (pos in vlist) {
	        rschema <- .jcast(schema$get(as.integer(pos)), new.class="org/apache/hadoop/hive/metastore/api/FieldSchema",check = FALSE, convert.array = FALSE)
	        if(rschema$getType() == "string") {
	        	rdata[[pos + 1]] <- character()
	        } else {
	        	rdata[[pos + 1]] <- numeric()
	        }
	        	
	        names(rdata)[pos + 1] <- rschema$getName()
	      }
	    }
    }

     result <- client$fetchN(as.integer(fetchsize))  
     list <- .jcast(result, new.class="java/util/List",check = FALSE, convert.array = FALSE)
     totalcount <- 0
     
     while(list$size() == fetchsize && (limit == -1 || totalcount < limit)) {
     
     	lapply(list, function(item) { 
     		item <- .jcast(item, new.class="java/lang/String",check = FALSE, convert.array = FALSE)
     		record <- strsplit(item$toString(),"\t")
     		for(i in seq.int(record[[1]])) {
     		    if(is.numeric(rdata[[i]])) {
     		    	rdata[[i]] <<- c(rdata[[i]],as.numeric(record[[1]][i]))	
     		    }else {
     				rdata[[i]] <<- c(rdata[[i]],record[[1]][i])	
     			}
     		}
     		
     		if(length(rdata) > length(record[[1]])) {
     		    gap <- length(rdata) - length(record[[1]])
	 			for(i in seq.int(length(record[[1]]) + 1,length(rdata))) {
	 				if(is.numeric(rdata[[i]])) {
	 					rdata[[i]] <<- c(rdata[[i]],NA)
	 				}else {
	 					rdata[[i]] <<- c(rdata[[i]],"")	
	 				}
	 			}
 			}
     	})
     	     	
     	totalcount <- totalcount + list$size()     
     	if(limit == -1 || totalcount < limit) {	
     		result <- client$fetchN(as.integer(fetchsize))
        	list <- .jcast(result, new.class="java/util/List",check = FALSE, convert.array = FALSE)
        }
     }
    
	if(limit == -1 || totalcount < limit) {
	    lapply(list, function(item) { 
	     		item <- .jcast(item, new.class="java/lang/String",check = FALSE, convert.array = FALSE)

	     		record <- strsplit(item$toString(),"\t")
	     		for(i in seq.int(record[[1]])) {
	    		    if(is.numeric(rdata[[i]])) {
	     		    	rdata[[i]] <<- c(rdata[[i]],as.numeric(record[[1]][i]))	
	     		    }else {
	     				rdata[[i]] <<- c(rdata[[i]],record[[1]][i])	
	     			}
		 		}

	     		if(length(rdata) > length(record[[1]])) {
	     		    gap <- length(rdata) - length(record[[1]])
		 			for(i in seq.int(length(record[[1]]) + 1,length(rdata))) {
		 				if(is.numeric(rdata[[i]])) {
		 					rdata[[i]] <<- c(rdata[[i]],NA)
		 				}else {
		 					rdata[[i]] <<- c(rdata[[i]],"")	
		 				}
		 			}
	 			}
	     })
     }

	 return(as.data.frame(rdata))

}

rhive.export <- function(exportname, hiveclient=rhive.defaults('hiveclient'), port = 6311, pos = -1, envir = .rhiveExportEnv, limit = 104857600) {
    
    .checkConnection(hiveclient)

	hosts <- hiveclient[[4]]

	for(rhost in hosts) {
	    rcon <- RSconnect(rhost, port)
	    
	    if(object.size(get(exportname,pos,envir)) < limit) {
	    	result <- try(RSassign(rcon,get(exportname,pos,envir),exportname), silent = FALSE)
	    	if(class(result) == "try-error") return(FALSE)
		}else {
			print("exceed limit object size")
		}
		
		command <- paste("save(",exportname,",file=paste(Sys.getenv('RHIVE_DATA')",",'/",exportname,".Rdata',sep=''))",sep="")
		RSeval(rcon,command)
		
		RSclose(rcon)
	}
	
	return(TRUE)

}

rhive.exportAll <- function(exportname, hiveclient=rhive.defaults('hiveclient'), port = 6311, pos = 1, envir = .rhiveExportEnv, limit = 104857600) {
    
    .checkConnection(hiveclient)
    
    hosts <- hiveclient[[4]]
    
    if(attr(envir,"name") <- 'no attribute' == "package:RHive") {
    	print("can not export 'package:RHive'")
    	return(FALSE)
    }
    
    list <- ls(NULL,pos,envir)
   
    for(rhost in hosts) {
    
        total_size <- 0
	    rcon <- RSconnect(rhost, port)
	    
	   for(item in list) {
	   		value <- get(item,pos,envir)
	        total_size <- total_size + object.size(value)
	        if(total_size < limit) {
	    		result <- try(RSassign(rcon,value,item), silent = FALSE)
	    		if(class(result) == "try-error") return(FALSE)
	    	}else {
				print("exceed limit object size")
			}
	    }
		
		command <- paste("save(list=ls(pattern=\"[^exportname]\")",",file=paste(Sys.getenv('RHIVE_DATA')",",'/",exportname,".Rdata',sep=''))",sep="")	
		RSeval(rcon,command)
	
		RSclose(rcon)
	
	}
	
	return(TRUE)
}

rhive.list.tables <- function(hiveclient=rhive.defaults('hiveclient')) {

	rhive.query("show tables",hiveclient=hiveclient)

}

rhive.desc.table <- function(tablename,detail=FALSE,hiveclient=rhive.defaults('hiveclient')) {

	if(!is.character(tablename))
		stop("argument type is wrong. tablename must be string type.")

    if(detail) {
    	tableInfo <- rhive.query(paste("describe extended",tablename),hiveclient=hiveclient)
    	return(tableInfo[[2]][length(rownames(tableInfo))])
    } else {
		rhive.query(paste("describe",tablename),hiveclient=hiveclient)
	}
}

rhive.load.table <- function(tablename, fetchsize = 40, limit = -1, hiveclient=rhive.defaults('hiveclient')) {

	if(!is.character(tablename))
		stop("argument type is wrong. tablename must be string type.")

	rhive.query(paste("select * from",tablename),fetchsize = fetchsize, limit = limit,hiveclient=hiveclient)

}

rhive.exist.table <- function(tablename, hiveclient=rhive.defaults('hiveclient')) {

	if(!is.character(tablename))
		stop("argument type is wrong. tablename must be string type.")

    tablelist <- try(rhive.list.tables(), silent = FALSE)
    if(class(tablelist) == "try-error") stop("fail to execute 'rhive.list.tables()'")
    
    loc <- (tablelist == tolower(tablename))
    if(length(tablelist[loc]) == 0)
    	return(FALSE)
    else
    	return(TRUE)
    	
}

rhive.napply <- function(tablename, FUN, ...,hiveclient =rhive.defaults('hiveclient')) {

	if(!is.character(tablename))
		stop("argument type is wrong. tablename must be string type.")

    colindex <- 0
    cols <- ""

    for(element in c(...)) {
         cols <- paste(cols,element,sep="")    
         if(colindex < length(c(...)) - 1)
         	cols <- paste(cols,",",sep="")
         colindex <- colindex + 1
    }
    
    if(length(c(...)) > 0)
    	cols <- paste(",",cols,sep="")
    
    exportname <- paste(tablename,"_napply",as.integer(Sys.time()),sep="")

	rhive.assign(exportname,FUN)
	rhive.exportAll(exportname,hiveclient)
	
	tmptable <- paste(exportname,"_table",sep="")
	
	query <- paste("CREATE TABLE ",tmptable," AS SELECT ","R('",exportname,"'",cols,",0.0) FROM ",tablename,sep="")
	
	rhive.query(query,hiveclient=hiveclient)
	
	tmptable

}

rhive.sapply <- function(tablename, FUN, ..., hiveclient =rhive.defaults('hiveclient')) {

	if(!is.character(tablename))
		stop("argument type is wrong. tablename must be string type.")

    colindex <- 0
    cols <- ""

    for(element in c(...)) {
         cols <- paste(cols,element,sep="")    
         if(colindex < length(c(...)) - 1)
         	cols <- paste(cols,",",sep="")
         colindex <- colindex + 1
    }
    
    if(length(c(...)) > 0)
    	cols <- paste(",",cols,sep="")
    
    exportname <- paste(tablename,"_sapply",as.integer(Sys.time()),sep="")

	rhive.assign(exportname,FUN)
	rhive.exportAll(exportname,hiveclient)
	
	tmptable <- paste(exportname,"_table",sep="")
	
	query <- paste("CREATE TABLE ",tmptable," AS SELECT ","R('",exportname,"'",cols,",'') FROM ",tablename,sep="")
	
	rhive.query(query,hiveclient=hiveclient)

	tmptable
}

rhive.aggregate <- function(tablename, hiveFUN, ..., groups = NULL , hiveclient =rhive.defaults('hiveclient')) {
    
	if(!is.character(tablename))
		stop("argument type is wrong. tablename must be string type.")    

    colindex <- 0
    cols <- ""

    for(element in c(...)) {
         cols <- paste(cols,element,sep="")    
         if(colindex < length(c(...)) - 1)
         	cols <- paste(cols,",",sep="")
         colindex <- colindex + 1
    }
    
    tmptable <- paste(tablename,"_aggregate",as.integer(Sys.time()),sep="")
    
    if(is.null(groups))
		rhive.query(paste("CREATE TABLE ",tmptable," AS SELECT ", hiveFUN ,"(",cols,") FROM ",tablename,sep=""),hiveclient=hiveclient)
	else {
		
		index <- 0
		gs <- ""
	
		for(element in groups) {
			gs <- paste(gs,element,sep="")
			
			if(index < length(groups) -1)
				gs <- paste(gs,",",sep="")
				
			index <- index + 1
		}
		
		query <- paste("CREATE TABLE ",tmptable," AS SELECT ", hiveFUN ,"(",cols,") FROM ",tablename," GROUP BY ",gs,sep="")

	    rhive.query(query,hiveclient=hiveclient)
	}
	
	
	tmptable

}

rhive.mapapply <- function(tablename, mapperFUN, mapinput=NULL, mapoutput=NULL, by=NULL, args=NULL, buffersize=-1L, verbose=FALSE, hiveclient =rhive.defaults('hiveclient')) {

	rhive.mrapply(tablename,mapperFUN=mapperFUN,mapinput=mapinput,mapoutput=mapoutput,by=by,mapper_args=args, reducer_args=NULL, buffersize=buffersize, verbose=verbose,hiveclient=hiveclient)

}

rhive.reduceapply <- function(tablename, reducerFUN, reduceinput=NULL,reduceoutput=NULL, args=NULL, buffersize=-1L, verbose=FALSE, hiveclient =rhive.defaults('hiveclient')) {

	rhive.mrapply(tablename,reducerFUN=reducerFUN,reduceinput=reduceinput,reduceoutput=reduceoutput,mapper_args=NULL, reducer_args=args, buffersize=buffersize, verbose=verbose,hiveclient=hiveclient)

}

rhive.mrapply <- function(tablename, mapperFUN, reducerFUN, mapinput=NULL, mapoutput=NULL, by=NULL, reduceinput=NULL,reduceoutput=NULL, mapper_args=NULL, reducer_args=NULL, buffersize=-1L, verbose=FALSE, hiveclient =rhive.defaults('hiveclient')) {

	if(!is.character(tablename))
		stop("argument type is wrong. tablename must be string type.")

	hql <- ""

	exportname <- paste("rhive",as.integer(Sys.time()),sep="")
	rhive.script.export(exportname, mapperFUN,reducerFUN, mapper_args, reducer_args, buffersize=buffersize)

	client <- .jcast(hiveclient[[1]], new.class="org/apache/hadoop/hive/service/HiveClient",check = FALSE, convert.array = FALSE)

	mapScript <- paste(exportname,".mapper",sep="")
	reduceScript <- paste(exportname,".reducer",sep="")

	micols <- "*"
	mocols <- NULL
	if(!is.null(mapinput)) {
	    micols <- rhive.as.string(mapinput)
	    mocols <- micols
    }
 	if(!is.null(mapoutput)) {
 	    mocols <- rhive.as.string(mapoutput)
    }
    
	ricols <- "*"
	if(!is.null(reduceinput)) {
		if(is.null(mapperFUN)) {
			ricols <- rhive.as.string(reduceinput)
		} else {
	    	ricols <- rhive.as.string(reduceinput,prefix="map_output.")
	    }
    }
    rocols <- NULL
    
 	if(!is.null(reduceoutput)) {
 	    rocols <- rhive.as.string(reduceoutput)
    }
  
    if(is.null(mapperFUN)) {
    
    	hql <- paste("FROM",tablename,sep=" ")
    
    }else {
    
        client$execute(.jnew("java/lang/String",paste("add file hdfs:///rhive/script/",mapScript,sep="")))
    
        if(is.function(mapperFUN)) {
    
	        hql <- paste("FROM (FROM",tablename,"MAP",micols,"USING",paste("'",mapScript,"'",sep=""),sep=" ")
	    
	    	if(!is.null(mocols)) {
	    		hql <- paste(hql,"as",mocols,sep=" ")
	    	}
	    	
	    	if(is.null(by)) {
	    		hql <- paste(hql,") map_output",sep=" ")
	    	} else {
	    		hql <- paste(hql,"cluster by",by,") map_output",sep=" ")
	    	}
    	
    	}else {
    		hql <- paste("FROM (",mapperFUN,") map_output",sep=" ")
    	}
    
    }
    
    if(!is.null(reducerFUN)) {
        
	    client$execute(.jnew("java/lang/String",paste("add file hdfs:///rhive/script/",reduceScript,sep="")))
        
        hql <- paste(hql,"REDUCE",ricols,"USING",paste("'",reduceScript,"'",sep=""),sep=" ")

    	if(!is.null(rocols)) {
    		hql <- paste(hql,"as",rocols,sep=" ")
    	}
    
    }else {
    
    	if(is.null(mocols)) {
    		hql <- paste("SELECT *",hql,sep=" ")
    	}else{
        	hql <- paste("SELECT",mocols,hql,sep=" ")
        }
    
    }

    if(is.null(mapperFUN) && is.null(reducerFUN))
    	hql <- paste("SELECT * FROM ", tablename,sep="")

	if(verbose) 
		print(paste("HIVE-QUERY : ", hql,sep=""))

	resultSet <- rhive.query(hql,hiveclient=hiveclient)

	rhive.script.unexport(exportname)

	return(resultSet)
}

rhive.as.string <- function(columns,prefix=NULL) {

    colindex <- 0
    if(is.null(prefix)) {
    	cols <- ""
    }else {
    	cols <- prefix
    }
    for(element in columns) {
         cols <- paste(cols,element,sep="")    
         if(colindex < length(columns) - 1) {
         	if(is.null(prefix)) {
         		cols <- paste(cols,",",sep="")
         	}else {
         		cols <- paste(cols,",",prefix,sep="")
         	}
         }
         colindex <- colindex + 1
    }

    return(cols)
}


#
# Function Language Style API
#

hiveConnect <- function(host,port=10000) {
	rhive.connect(host,port)
}

hiveClose <- function(hiveclient=rhive.defaults('hiveclient')) {
	rhive.close(hiveclient)
}

hiveQuery <- function(query, fetchsize = 40, limit = -1, hiveclient=rhive.defaults('hiveclient')) {
	rhive.query(query,fetchsize,limit,hiveclient)
}

hiveExport <- function(exportname, hosts = "localhost", port = 6311, pos = -1, envir = .rhiveExportEnv, limit = 104857600) {
	rhive.export(exportname, hosts, port, pos, envir, limit)
}

hiveExportAll <- function(exportname, hosts = "localhost", port = 6311, pos = 1, envir = .rhiveExportEnv, limit = 104857600) {
	rhive.exportAll(exportname, hosts, port, pos, envir, limit)
}

hiveListTables <- function(hiveclient=rhive.defaults('hiveclient')) {
	rhive.list.tables(hiveclient)
}

hiveDescTable <- function(tablename,detail=FALSE,hiveclient=rhive.defaults('hiveclient')) {
	rhive.desc.table(tablename, detail, hiveclient)
}

hiveLoadTable <- function(tablename, limit = -1, hiveclient=rhive.defaults('hiveclient')) {
	rhive.load.table(tablename, limit, hiveclient)
}

hiveAssign <- function(name, value) {
        rhive.assign(name,value)
}

hiveRm <- function(name) {
        rhive.rm(name)
}
