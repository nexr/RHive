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


.rhive.hdfs.connect <- function(defaultFS=NULL) {
  if (is.null(defaultFS)) {
    defaultFS <- .DEFAULT_FS()
  }

  FSUtils <- .j2r.FSUtils()
  ok <- FSUtils$checkFileSystem(defaultFS)
  if (!ok) {
    stop(sprintf("Failed to connect to %s.", defaultFS))
  }

 .setEnv("DEFAULT_FS", defaultFS)
  return (TRUE)
}

.rhive.hdfs.ls <- function(path="/") {
  FSUtils <- .j2r.FSUtils()
  lst <- FSUtils$ls(path, .DEFAULT_FS())
  lst <- lapply(lst, FUN=function(x) { as.list(x) })
  lst[[4]] <- as.numeric(lst[[4]])

  if (length(lst[[1]]) == 0) {
    return(NULL)
  }

  df <- as.data.frame(lst)

  rownames(df) <- NULL
  colnames(df) <- c("permission", "owner", "group", "length", "modify-time", "file")
  return (df)
}

.rhive.hdfs.du <- function(path="/", summary=FALSE) {
  FSUtils <- .j2r.FSUtils()

  if (summary) {
    lst <- FSUtils$dus(path, .DEFAULT_FS())
  } else {
    lst <- FSUtils$du(path, .DEFAULT_FS())
  }

  lst <- lapply(lst, FUN=function(x) { as.list(x) })
  lst[[1]] <- as.numeric(lst[[1]])

  if (length(lst[[1]]) == 0) {
    return(NULL)
  }

  df <- as.data.frame(lst)

  rownames(df) <- NULL
  colnames(df) <- c("length", "file")

  return (df)
}

.rhive.save <- function(..., file, envir=parent.frame()) {
  tmpFile <- .TMP_FILE("RHIVE.SAVE", sub=TRUE)

  save(..., file=tmpFile, envir=envir)
  tryCatch ( {
     .rhive.hdfs.put(tmpFile, file)
    }, error=function(e) {
      unlink(tmpFile)
      return (FALSE)
    }
  )
  
  unlink(tmpFile)
  return(TRUE)
}

.rhive.load <- function(file, envir=parent.frame()) {
  tmpFile <- .TMP_FILE("RHIVE.LOAD", sub=TRUE)
 .rhive.hdfs.get(file, tmpFile)
  tryCatch ( {
      load(file=tmpFile, envir=envir)
    }, error=function(e) {
      unlink(tmpFile)
      return (FALSE)
    }
  )
  
  unlink(tmpFile)
  return(TRUE)
}

.rhive.hdfs.put <- function(src, dst, srcDel=FALSE, overwrite=FALSE) {
  FSUtils <- .j2r.FSUtils()
  FSUtils$copyFromLocalFile(srcDel, overwrite, src, dst, .DEFAULT_FS())
  return(TRUE)
}

.rhive.hdfs.get <- function(src, dst, srcDel=FALSE) {
  FSUtils <- .j2r.FSUtils()
  FSUtils$copyToLocalFile(srcDel, src, dst, .DEFAULT_FS())
  return(TRUE)
}

.rhive.hdfs.rm <- function(...) {
  FSUtils <- .j2r.FSUtils()
  res <- c()
  for (target in c(...)) {
     res <- c(res, FSUtils$delete(target, .DEFAULT_FS()))
  }
  return (res)
}

.rhive.hdfs.rename <- function(src, dst) {
  FSUtils <- .j2r.FSUtils()
  return (FSUtils$rename(src, dst, .DEFAULT_FS()))
}


.rhive.hdfs.exists <- function(path) {
  FSUtils <- .j2r.FSUtils()
  return (FSUtils$exists(path, .DEFAULT_FS()))
}

.rhive.hdfs.mkdirs <- function(path) {
  FSUtils <- .j2r.FSUtils()
  return (FSUtils$mkdirs(path, .DEFAULT_FS()))
}

.rhive.hdfs.cat <- function(path) {
  FSUtils <- .j2r.FSUtils()
  FSUtils$cat(path, .DEFAULT_FS())
  invisible()
}

.rhive.hdfs.tail <- function(path) {
  FSUtils <- .j2r.FSUtils()
  FSUtils$tail(path, .DEFAULT_FS())
  invisible()
}

.rhive.hdfs.chmod <- function(option, path, recursive=FALSE) {
  FSUtils <- .j2r.FSUtils()
  FSUtils$chmod(path, option, recursive, .DEFAULT_FS())
  invisible()
}


.rhive.hdfs.chown <- function(option, path, recursive=FALSE) {
  FSUtils <- .j2r.FSUtils()
  FSUtils$chown(path, option, recursive, .DEFAULT_FS())
  invisible()
}

.rhive.hdfs.chgrp <- function(option, path, recursive=FALSE) {
  FSUtils <- .j2r.FSUtils()
  FSUtils$chgrp(path, option, recursive, .DEFAULT_FS())
  invisible()
}

.rhive.hdfs.close <- function() {
 .unsetEnv("DEFAULT_FS")
  return (TRUE)
}

.rhive.write.table <- function(data, tableName, sep=",", naString=NULL, rowName=FALSE, rowNameColumn="rowname") {
  hiveClient <- .getHiveClient()
  
  if (!is.data.frame(data)) {
    stop("data should be a data frame")
  }

  if (rowName) {
    data <- cbind(row.names(data), data)
    names(data)[1L] <- rowNameColumn
  }
  
  types <- sapply(data, typeof)
  facs <- sapply(data, is.factor)
  isReal <- (types == "double")
  isInt <- (types == "integer") & !facs
  isLogi <- (types == "logical")
  
  colSpecs <- rep("STRING", length(data))
  colSpecs[isReal] <- "DOUBLE"
  colSpecs[isInt] <- "INT"
  colSpecs[isLogi] <- "BOOLEAN"
  
  names(colSpecs) <- names(data)
  if (rowName) {
    if (length(colSpecs) > length(data)) {
      colSpecs <- colSpecs[2 : length(colSpecs)]
    }
  }

  data[is.na(data)] <- if (is.null(naString)) "NULL" else naString[1L]
  
  if (.rhive.exist.table(tableName)) {
    stop(sprintf("%s already exists.", sQuote(tableName)))
  }
  
  tmpFile <- .TMP_FILE(tableName, sub=TRUE)
  if (!file.exists(tmpFile)) {
    file.create(tmpFile)
  }

  write.table(data, file=tmpFile, quote=FALSE, row.names=FALSE, col.names=FALSE, sep=sep)

  hdfsPath <- sprintf("%s/%s", .FS_DATA_DIR(), basename(tmpFile))
 .rhive.hdfs.put(tmpFile, hdfsPath, srcDel=TRUE, overwrite=TRUE)
  
  query <- .generateCreateQuery(tableName, colSpecs, sep=sep)
  hiveClient$execute(query)

  query <- .generateLoadDataQuery(tableName, hdfsPath) 
  hiveClient$execute(query)

  return (tableName)
}

.rhive.export.script <- function(exportName, mapper=NULL, reducer=NULL, mapArgs=NULL, reduceArgs=NULL, bufferSize=-1L) {

  mapperScript <- "NULL"
  reducerScript <- "NULL"

  if (!is.null(mapper)) {
    template <- paste(system.file(package="RHive"), "resource", "_mapper.template", sep=.Platform$file.sep)

    tmpFile <- .TMP_FILE("rhive.mapper")
   .generateScript(mapper, tmpFile, template, "map", mapArgs, bufferSize)

    mapperScript <- .FS_MAPPER_SCRIPT_PATH(exportName)
   .rhive.hdfs.put(tmpFile, mapperScript, srcDel=TRUE, overwrite=TRUE)
  }
  
  if (!is.null(reducer)) {
    template <- paste(system.file(package="RHive"),"resource","_reducer.template",sep=.Platform$file.sep)
    tmpFile <- .TMP_FILE("rhive.reducer")
   .generateScript(reducer, tmpFile, template, "reduce", reduceArgs, bufferSize)
 
    reducerScript <- .FS_REDUCER_SCRIPT_PATH(exportName)
   .rhive.hdfs.put(tmpFile, reducerScript, srcDel=TRUE, overwrite=TRUE);
  }
  
  return (c(mapperScript, reducerScript))
}

.rhive.unexport.script <- function(exportName) {
  mapperScript <- .FS_MAPPER_SCRIPT_PATH(exportName)
  reducerScript <- .FS_REDUCER_SCRIPT_PATH(exportName)
  
  if (.rhive.hdfs.exists(mapperScript)) {
   .rhive.hdfs.rm(mapperScript)
  }
  
  if (.rhive.hdfs.exists(reducerScript)) {
   .rhive.hdfs.rm(reducerScript)
  }
  
  return(TRUE)
}

.rhive.script.export <- .rhive.export.script
.rhive.script.unexport <- .rhive.unexport.script

.rhive.hdfs.info <- function(path) {
  FSUtils <- .j2r.FSUtils()
  FSUtils$info(path)
}

.generateCreateQuery <- function (tableName, colSpecs, sep=",") {
  colnames <- gsub("[^[:alnum:]_]+", "", names(colSpecs))

  if (any(duplicated(tolower(colnames))) == TRUE) {
    stop(paste("Hive doesn't support case-sensitive column-name :",paste(colnames,collapse=",")))
  }

  entries <- paste(colnames, colSpecs)
  
  return (sprintf("CREATE TABLE %s ( %s ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '%s'", tableName, paste(entries, collapse=", "), sep))
}

.generateLoadDataQuery <- function (tableName, hdfsPath) {
  return (sprintf("LOAD DATA INPATH '%s' OVERWRITE INTO TABLE %s", hdfsPath, tableName)) 
}

.generateScript <- function(x, output, script, name, args, bufferSize) {
  customFunction <- paste(deparse(x), collapse="\n")
  
  prefix <- "#!/usr/bin/env Rscript\n"
  buffer <- sprintf("bufferSize <- %s\n", bufferSize)
  
  if (is.null(args)) {
    args <- ""
  }

  userArgs <- sprintf("args <- '%s'\n", as.character(args))
  fname <- sprintf("%s <- ", name)
  
  cat(sprintf("%s%s%s%s%s", prefix, buffer, userArgs, fname, customFunction), file=output, sep="\n", append=FALSE)
  
  lines <- readLines(script)
  
  for (line in lines) {
    cat(sprintf("%s", line), file=output, sep="\n", append=TRUE)
  }
  
  status <- system(sprintf("chmod 775 %s", output), ignore.stderr=TRUE)
  
  if (status != 0) {
    warning("No executable found")
    invisible(FALSE)
  }

  invisible(output)
}

.localCleanup <- function(files){
  if (all(file.exists(files))) {
    unlink(files)
  }
}
