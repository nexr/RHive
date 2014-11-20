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


.handleErr <- function(e) {
 .setErr(e)  
  stop(e$message, call.=FALSE)
}

.setErr <- function(e) {
  assign("last-err", e, envir=.rhiveErrEnv)
}


lastErr <- function() {
  RHive:::.rhiveErrEnv[["last-err"]]
}


rhive.init <- function(hiveHome=NULL, hiveLib=NULL, hadoopHome=NULL, hadoopConf=NULL, hadoopLib=NULL, verbose=FALSE) {
  tryCatch ( {
     .rhive.init(hiveHome=hiveHome, hiveLib=hiveLib, hadoopHome=hadoopHome, hadoopConf=hadoopConf, hadoopLib=hadoopLib, verbose=verbose) 
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.assign <- function(name, value) {
  tryCatch ( {
     .rhive.assign(name=name, value=value)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.assign.export <- function(name, value) {
  tryCatch ( {
     .rhive.assign.export(name=name, value=value)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.rm <- function(name) {
  tryCatch ( {
     .rhive.rm(name=name)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.rm.export <- function(name) {
  tryCatch ( {
     .rhive.rm.export(name=name)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.env <- function(ALL=FALSE) {
  tryCatch ( {
     .rhive.env(ALL=ALL)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.connect <- function(host="127.0.0.1", port=10000, hiveServer2=NA, defaultFS=NULL, updateJar=FALSE, user=NULL, password=NULL, db="default", properties = character(0)) {
  tryCatch ( {
     .rhive.connect(host=host, port=port, hiveServer2=hiveServer2, defaultFS=defaultFS, updateJar=updateJar, user=user, password=password,db,properties)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.close <- function() {
  tryCatch ( {
     .rhive.close()
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.big.query <- function(query ,fetchSize=50, limit=-1, memLimit=64*1024*1024) {
  tryCatch ( {
     .rhive.big.query(query=query ,fetchSize=fetchSize, limit=limit, memLimit=memLimit)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.query <- function(query, fetchSize=50, limit=-1) {
  tryCatch ( {
     .rhive.query(query=query, fetchSize=fetchSize, limit=limit)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.execute <- function(query) {
  tryCatch ( {
     .rhive.execute(query=query)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.export <- function(exportName, pos=-1, limit=100*1024*1024, ALL=FALSE) {
  tryCatch ( {
     .rhive.export(exportName=exportName, pos=pos, limit=limit, ALL=ALL)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.exportAll <- function(exportName, pos=1, limit=100*1024*1024) {
  tryCatch ( {
     .rhive.exportAll(exportName=exportName, pos=pos, limit=limit)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.list.udfs <- function() {
  tryCatch ( {
     .rhive.list.udfs()
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.rm.udf <- function(exportName) {
  tryCatch ( {
     .rhive.rm.udf(exportName=exportName)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.list.databases <- function(pattern) {
  tryCatch ( {
     .rhive.list.databases(pattern=pattern)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.show.databases <- rhive.list.databases

rhive.use.database <- function(databaseName) {
  tryCatch ( {
     .rhive.use.database(databaseName=databaseName)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.list.tables <- function(pattern) {
  tryCatch ( {
     .rhive.list.tables(pattern=pattern)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.show.tables <- rhive.list.tables

rhive.desc.table <- function(tableName, detail=FALSE) {
  tryCatch ( {
     .rhive.desc.table(tableName=tableName, detail=detail)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.load.table <- function(tableName, fetchSize=50, limit=-1) {
  tryCatch ( {
     .rhive.load.table(tableName=tableName, fetchSize=fetchSize, limit=limit)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.load.table2 <- function(tableName, limit=-1, remote=TRUE) {
  tryCatch ( {
     .rhive.load.table2(tableName=tableName, limit=limit, remote=remote)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.exist.table <- function(tableName) {
  tryCatch ( {
     .rhive.exist.table(tableName=tableName)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.set <- function(key, value) {
  tryCatch ( {
     .rhive.set(key=key, value=value)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.unset <- function(key) {
  tryCatch ( {
     .rhive.unset(key=key)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.napply <- function(tableName, FUN, ..., forcedRef=TRUE) {
  tryCatch ( {
     .rhive.napply(tableName=tableName, FUN=FUN, ..., forcedRef=forcedRef)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.sapply <- function(tableName, FUN, ..., forcedRef=TRUE) {
  tryCatch ( {
     .rhive.sapply(tableName=tableName, FUN=FUN, ..., forcedRef=forcedRef)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.aggregate <- function(tableName, hiveFUN, ..., groups=NULL, forcedRef=TRUE) {
  tryCatch ( {
     .rhive.aggregate(tableName=tableName, hiveFUN=hiveFUN, ..., groups=groups, forcedRef=forcedRef)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.mapapply <- function(tableName, mapperFUN, mapInput=NULL, mapOutput=NULL, by=NULL, args=NULL, bufferSize=-1L, verbose=FALSE, forcedRef=TRUE) {
  tryCatch ( {
     .rhive.mapapply(tableName=tableName, mapperFUN=mapperFUN, mapInput=mapInput, mapOutput=mapOutput, by=by, args=args, bufferSize=bufferSize, verbose=verbose, forcedRef=forcedRef)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.reduceapply <- function(tableName, reducerFUN, reduceInput=NULL,reduceOutput=NULL, args=NULL, bufferSize=-1L, verbose=FALSE, forcedRef=TRUE) {
  tryCatch ( {
     .rhive.reduceapply(tableName=tableName, reducerFUN=reducerFUN, reduceInput=reduceInput, reduceOutput=reduceOutput, args=args, bufferSize=bufferSize, verbose=verbose, forcedRef=forcedRef)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.mrapply <- function(tableName, mapperFUN, reducerFUN, mapInput=NULL, mapOutput=NULL, by=NULL, reduceInput=NULL,reduceOutput=NULL, mapperArgs=NULL, reducerArgs=NULL, bufferSize=-1L, verbose=FALSE, forcedRef=TRUE) {
  tryCatch ( {
     .rhive.mrapply(tableName=tableName, mapperFUN=mapperFUN, reducerFUN=reducerFUN, mapInput=mapInput, mapOutput=mapOutput, by=by, reduceInput=reduceInput, reduceOutput=reduceOutput, mapperArgs=mapperArgs, reducerArgs=reducerArgs, bufferSize=bufferSize, verbose=verbose, forcedRef=forcedRef)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.drop.table <- function(tableName, list) {
  tryCatch ( {
     .rhive.drop.table(tableName=tableName, list=list)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.size.table <- function(tableName) {
  tryCatch ( {
     .rhive.size.table(tableName=tableName)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}




rhive.hdfs.ls <- function(path="/") {
  tryCatch ( {
     .rhive.hdfs.ls(path=path)
    }, error=function(e) {
     .handleErr(e)
    }
  )
} 

rhive.hdfs.du <- function(path="/", summary=FALSE) {
  tryCatch ( {
     .rhive.hdfs.du(path=path, summary=summary)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.hdfs.dus <- function(path="/") {
  rhive.hdfs.du(path=path, summary=TRUE)
}

rhive.save <- function(..., file, envir=parent.frame()) {
  tryCatch ( {
     .rhive.save(..., file=file, envir=envir)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}
 
rhive.load <- function(file, envir=parent.frame()) {
  tryCatch ( {
     .rhive.load(file=file, envir=envir)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.hdfs.put <- function(src, dst, srcDel=FALSE, overwrite=FALSE) {
  tryCatch ( {
     .rhive.hdfs.put(src=src, dst=dst, srcDel=srcDel, overwrite=overwrite)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.hdfs.get <- function(src, dst, srcDel=FALSE) {
  tryCatch ( {
     .rhive.hdfs.get(src=src, dst=dst, srcDel=srcDel)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.hdfs.rm <- function(...) {
  tryCatch ( {
     .rhive.hdfs.rm(...)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.hdfs.rename <- function(src, dst) {
  tryCatch ( {
     .rhive.hdfs.rename(src=src, dst=dst)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.hdfs.exists <- function(path) {
  tryCatch ( {
     .rhive.hdfs.exists(path=path)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.hdfs.mkdirs <- function(path) {
  tryCatch ( {
     .rhive.hdfs.mkdirs(path=path)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.hdfs.cat <- function(path) {
  tryCatch ( {
     .rhive.hdfs.cat(path=path)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.hdfs.tail <- function(path) {
  tryCatch ( {
     .rhive.hdfs.tail(path=path)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.hdfs.chmod <- function(option, path, recursive=FALSE) {
  tryCatch ( {
     .rhive.hdfs.chmod(option=option, path=path, recursive=recursive)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.hdfs.chown <- function(option, path, recursive=FALSE) {
  tryCatch ( {
     .rhive.hdfs.chown(option=option, path=path, recursive=recursive)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.hdfs.chgrp <- function(option, path, recursive=FALSE) {
  tryCatch ( {
     .rhive.hdfs.chgrp(option=option, path=path, recursive=recursive)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}


rhive.write.table <- function(data, tableName, sep=",", naString=NULL, rowName=FALSE, rowNameColumn="rowname") {
  tryCatch ( {
     .rhive.write.table(data=data, tableName=tableName, sep=sep, naString=naString, rowName=rowName, rowNameColumn=rowNameColumn)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.export.script <- function(exportName, mapper=NULL, reducer=NULL, mapArgs=NULL, reduceArgs=NULL, bufferSize=-1L) {
  tryCatch ( {
     .rhive.export.script(exportName=exportName, mapper=mapper, reducer=reducer, mapArgs=mapArgs, reduceArgs=reduceArgs, bufferSize=bufferSize)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.unexport.script <- function(exportName) {
  tryCatch ( {
     .rhive.unexport.script(exportName=exportName)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.script.export <- function(exportName, mapper=NULL, reducer=NULL, mapArgs=NULL, reduceArgs=NULL, bufferSize=-1L) {
  tryCatch ( {
     .rhive.script.export(exportName=exportName, mapper=mapper, reducer=reducer, mapArgs=mapArgs, reduceArgs=reduceArgs, bufferSize=bufferSize)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.script.unexport <- function(exportName) {
  tryCatch ( {
     .rhive.script.unexport(exportName=exportName)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.hdfs.info <- function(path) {
  tryCatch ( {
     .rhive.hdfs.info(path=path)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}

rhive.login <- function(keytab,principal,hostname) {
  tryCatch ( {
     .rhive.login(keytab=keytab,principal=principal,hostname=hostname)
    }, error=function(e) {
     .handleErr(e)
    }
  )
}
