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


.rhive.call <- function() {
  tryCatch ( {
      fun <- as.character(sys.calls()[[sys.nframe()-1]])
      call <- sys.calls()[[sys.nframe()-1]]
      call[[1]] <- as.name(sprintf(".%s", fun))
      eval(call)
    }, error=function(e) {
     .setErr(e)  
      stop(e$message)
    }
  )
}

.setErr <- function(e) {
  assign("last-err", e, envir=.rhiveErrEnv)
}


lastErr <- function() {
  RHive:::.rhiveErrEnv[["last-err"]]
}


rhive.init <- function(hiveHome=NULL, hiveLib=NULL, hadoopHome=NULL, hadoopConf=NULL, hadoopLib=NULL, verbose=FALSE) {
  .rhive.call()
}

rhive.assign <- function(name, value) {
  .rhive.call()
}

rhive.assign.export <- function(name, value) {
  .rhive.call()
}

rhive.rm <- function(name) {
  .rhive.call()
}

rhive.rm.export <- function(name) {
  .rhive.call()
}

rhive.env <- function(ALL=FALSE) {
  .rhive.call()
}

rhive.connect <- function(host="127.0.0.1", port=10000, hiveServer2=NA, defaultFS=NULL, updateJar=FALSE) {
  .rhive.call()
}

rhive.close <- function() {
  .rhive.call()
}

rhive.big.query <- function(query ,fetchSize=50, limit=-1, memLimit=64*1024*1024) {
  .rhive.call()
}

rhive.query <- function(query, fetchSize=50, limit=-1) {
  .rhive.call()
}

rhive.execute <- function(query) {
  .rhive.call()
}

rhive.export <- function(exportName, pos=-1, limit=100*1024*1024, ALL=FALSE) {
  .rhive.call()
}

rhive.exportAll <- function(exportName, pos=1, limit=100*1024*1024) {
  .rhive.call()
}

rhive.list.udfs <- function() {
  .rhive.call()
}

rhive.rm.udf <- function(exportName) {
  .rhive.call()
}

rhive.list.databases <- function(pattern) {
  .rhive.call()
}

rhive.show.databases <- rhive.list.databases

rhive.use.database <- function(databaseName) {
  .rhive.call()
}

rhive.list.tables <- function(pattern) {
  .rhive.call()
}

rhive.show.tables <- rhive.list.tables

rhive.desc.table <- function(tableName, detail=FALSE) {
  .rhive.call()
}

rhive.load.table <- function(tableName, fetchSize=50, limit=-1) {
  .rhive.call()
}

rhive.load.table2 <- function(tableName, limit=-1, remote=TRUE) {
  .rhive.call()
}

rhive.exist.table <- function(tableName) {
  .rhive.call()
}

rhive.set <- function(key, value) {
  .rhive.call()
}

rhive.unset <- function(key) {
  .rhive.call()
}

rhive.napply <- function(tableName, FUN, ..., forcedRef=TRUE) {
  .rhive.call()
}

rhive.sapply <- function(tableName, FUN, ..., forcedRef=TRUE) {
  .rhive.call()
}

rhive.aggregate <- function(tableName, hiveFUN, ..., groups=NULL , forcedRef=TRUE) {
  .rhive.call()
}

rhive.mapapply <- function(tableName, mapperFUN, mapInput=NULL, mapOutput=NULL, by=NULL, args=NULL, bufferSize=-1L, verbose=FALSE, forcedRef=TRUE) {
  .rhive.call()
}

rhive.reduceapply <- function(tableName, reducerFUN, reduceInput=NULL,reduceOutput=NULL, args=NULL, bufferSize=-1L, verbose=FALSE, forcedRef=TRUE) {
  .rhive.call()
}

rhive.mrapply <- function(tableName, mapperFUN, reducerFUN, mapInput=NULL, mapOutput=NULL, by=NULL, reduceInput=NULL,reduceOutput=NULL, mapperArgs=NULL, reducerArgs=NULL, bufferSize=-1L, verbose=FALSE, forcedRef=TRUE) {
  .rhive.call()
}

rhive.drop.table <- function(tableName, list) {
  .rhive.call()
}

rhive.size.table <- function(tableName) {
  .rhive.call()
}




rhive.hdfs.ls <- function(path="/") {
  .rhive.call()
} 

rhive.hdfs.du <- function(path="/", summary=FALSE) {
  .rhive.call()
}

rhive.hdfs.dus <- function(path="/") {
  rhive.hdfs.du(path, summary=TRUE)
}

rhive.save <- function(..., file, envir=parent.frame()) {
  .rhive.call()
}
 
rhive.load <- function(file, envir=parent.frame()) {
  .rhive.call()
}

rhive.hdfs.put <- function(src, dst, srcDel=FALSE, overwrite=FALSE) {
  .rhive.call()
}

rhive.hdfs.get <- function(src, dst, srcDel=FALSE) {
  .rhive.call()
}

rhive.hdfs.rm <- function(...) {
  .rhive.call()
}

rhive.hdfs.rename <- function(src, dst) {
  .rhive.call()
}

rhive.hdfs.exists <- function(path) {
  .rhive.call()
}

rhive.hdfs.mkdirs <- function(path) {
  .rhive.call()
}

rhive.hdfs.cat <- function(path) {
  .rhive.call()
}

rhive.hdfs.tail <- function(path) {
  .rhive.call()
}

rhive.hdfs.chmod <- function(option, path, recursive=FALSE) {
  .rhive.call()
}

rhive.hdfs.chown <- function(option, path, recursive=FALSE) {
  .rhive.call()
}

rhive.hdfs.chgrp <- function(option, path, recursive=FALSE) {
  .rhive.call()
}

rhive.write.table <- function(data, tableName=NULL, sep=",", naString=NULL) {
  .rhive.call()
}

rhive.export.script <- function(exportName, mapper=NULL, reducer=NULL, mapArgs=NULL, reduceArgs=NULL, bufferSize=-1L) {
  .rhive.call()
}

rhive.unexport.script <- function(exportName) {
  .rhive.call()
}

rhive.script.export <- function(exportName, mapper=NULL, reducer=NULL, mapArgs=NULL, reduceArgs=NULL, bufferSize=-1L) {
  .rhive.call()
}

rhive.script.unexport <- function(exportName) {
  .rhive.call()
}

rhive.hdfs.info <- function(path) {
  .rhive.call()
}
