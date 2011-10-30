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


rhive.init <- function(hive=NULL,libs=NULL,verbose=FALSE){

  if(is.null(hive)) hive <- Sys.getenv("HIVE_HOME")
  if(hive=="")
    stop(sprintf("One or both of HIVE_HOME(%s) is missing. Please set them and rerun",
                 Sys.getenv("HIVE_HOME")))
  if(is.null(libs)) libs <- sprintf("%s/lib",hive)
  if(verbose) cat(sprintf("Detected hive=%s and libs=%s\n",hive,libs))
  hive.CP <- c(list.files(libs,full.names=TRUE,pattern="jar$",recursive=FALSE)
               ,list.files(paste(system.file(package="RHive"),"java",sep=.Platform$file.sep),pattern="jar$",full=T)
               )
  assign("classpath",hive.CP,envir=.rhiveEnv)
  .jinit(classpath= hive.CP)
  
  options(show.error.messages = TRUE)
}

rhive.defaults <- function(arg){
  if(missing(arg)){
    as.list(.rhiveEnv)
  } else { 
  	RHive:::.rhiveEnv[[arg]]
  }
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


rhive.connect <- function(host="127.0.0.1",port=10000) {

	 TSocket <- J("org.apache.thrift.transport.TSocket")
     TProtocol <- J("org.apache.thrift.protocol.TProtocol")
     HiveClient <- J("org.apache.hadoop.hive.service.HiveClient")
     hivecon <- .jnew("org/apache/thrift/transport/TSocket",.jnew("java/lang/String",host),as.integer(port))
     tpt <- .jnew("org/apache/thrift/protocol/TBinaryProtocol",.jcast(hivecon, new.class="org/apache/thrift/transport/TTransport",check = FALSE, convert.array = FALSE))
     client <- .jnew("org/apache/hadoop/hive/service/HiveClient",.jcast(tpt, new.class="org/apache/thrift/protocol/TProtocol",check = FALSE, convert.array = FALSE))
     
     hivecon$open()
     
     client$execute(.jnew("java/lang/String","add jar hdfs:///rhive/lib/rhive_udf.jar"))
     client$execute(.jnew("java/lang/String","create temporary function R as 'com.nexr.rhive.hive.udf.RUDF'"))
     client$execute(.jnew("java/lang/String","create temporary function RA as 'com.nexr.rhive.hive.udf.RUDAF'"))
     client$execute(.jnew("java/lang/String","create temporary function unfold as 'com.nexr.rhive.hive.udf.GenericUDTFUnFold'"))
     client$execute(.jnew("java/lang/String","create temporary function expand as 'com.nexr.rhive.hive.udf.GenericUDTFExpand'"))

     hiveclient <- c(client,hivecon,host)
     
     class(hiveclient) <- "rhive.client.connection"
     #reg.finalizer(hiveclient,function(r) {
     #     hivecon <- .jcast(r[[2]], new.class="org/apache/thrift/transport/TSocket",check = FALSE, convert.array = FALSE) 
     #     hivecon$close()	  
     #      print("call finalizer")
    # })
     assign('hiveclient',hiveclient,env=RHive:::.rhiveEnv)

}

rhive.close <- function(hiveclient=rhive.defaults('hiveclient')) {

	hivecon <- .jcast(hiveclient[[2]], new.class="org/apache/thrift/transport/TSocket",check = FALSE, convert.array = FALSE)
	hivecon$close()
	
	return(TRUE)
	
}

rhive.query <- function(query, fetchsize = 40, limit = -1, hiveclient=rhive.defaults('hiveclient')) {

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

rhive.export <- function(exportname, hosts = "127.0.0.1", port = 6311, pos = -1, envir = .rhiveExportEnv, limit = 104857600) {

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

rhive.exportAll <- function(exportname, hosts = "127.0.0.1", port = 6311, pos = 1, envir = .rhiveExportEnv, limit = 104857600) {
    
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

    if(detail) {
    	tableInfo <- rhive.query(paste("describe extended",tablename),hiveclient=hiveclient)
    	return(tableInfo[[2]][length(rownames(tableInfo))])
    } else {
		rhive.query(paste("describe",tablename),hiveclient=hiveclient)
	}
}

rhive.load.table <- function(tablename, limit = -1, hiveclient=rhive.defaults('hiveclient')) {

	rhive.query(paste("select * from",tablename),limit = limit,hiveclient=hiveclient)

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
