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



.rhive.init <- function(hiveHome=NULL, hiveLib=NULL, hadoopHome=NULL, hadoopConf=NULL, hadoopLib=NULL, verbose=FALSE) {

  if (is.null(hiveHome)) {
    hiveHome <- .getSysEnv("HIVE_HOME")
  }

  if (is.null(hadoopHome)) {
    hadoopHome <- .getSysEnv("HADOOP_HOME")
  }

  if (is.null(hiveHome) || is.null(hadoopHome)) {
    return (FALSE)
  }

  if (is.null(hadoopConf)) {
    hadoopConf <- .getSysEnv("HADOOP_CONF_DIR")
  }

  if (is.null(hiveLib)) {
    hiveLib <- .getSysEnv("HIVE_LIB_DIR")
  }
  
  if (is.null(hadoopLib)) {
    hadoopLib <- .getSysEnv("HADOOP_LIB_DIR")
  }

 .setEnv("HIVE_HOME", hiveHome)
 .setEnv("HADOOP_HOME", hadoopHome)

  if (!is.null(hiveLib)) {
   .setEnv("HIVE_LIB_DIR", hiveLib)
  }

  if (!is.null(hadoopLib)) {
   .setEnv("HADOOP_LIB_DIR", hadoopLib)
  }

  if (!is.null(hadoopConf)) {
   .setEnv("HADOOP_CONF_DIR", hadoopConf)
  }

  ver <- .getSysEnv("RHIVE_HIVESERVER_VERSION")
  if (!is.null(ver)) {
    .setEnv("HIVESERVER_VERSION", ver)
  }

  fsHome <- .getSysEnv("RHIVE_FS_HOME")
  if (!is.null(fsHome)) {
    .setEnv("FS_HOME", fsHome)
  }
  
  cp <- .getClasspath(hadoopHome, hadoopLib, hiveHome, hiveLib, hadoopConf)
 .setEnv("CLASSPATH", cp)

 .initJvm(cp)
 .setEnv("INITIALIZED", TRUE)

  if (verbose) {
    .rhive.env(ALL=TRUE)
  }

  options(show.error.messages=TRUE)
}

.getClasspath <- function(hadoopHome, hadoopLib, hiveHome, hiveLib, hadoopConf) {
  if (is.null(hadoopLib)) {
    hadoopLib <- .defaultLibDir(hadoopHome)
  }

  if (is.null(hiveLib)) {
    hiveLib <- .defaultLibDir(hiveHome)
  }

  if (is.null(hadoopConf)) {
    hadoopConf <- .defaultConfDir(hadoopHome)
  }

  cp <- c(list.files(paste(system.file(package="RHive"), "java", sep=.Platform$file.sep), pattern="jar$", full.names=TRUE))

  if (substr(hadoopLib, start=1, stop=nchar(hadoopHome)) != hadoopHome) {
    cp <- c(cp, list.files(hadoopLib, full.names=TRUE, pattern="jar$", recursive=TRUE))
  }

  cp <- c(cp, list.files(hadoopHome, full.names=TRUE, pattern="jar$", recursive=TRUE))
  cp <- c(cp, list.files(hiveLib, full.names=TRUE, pattern="jar$", recursive=TRUE))
  cp <- c(cp, hadoopConf)

  return (cp)
}

.initJvm <- function(cp) {
 .jinit(classpath=cp, parameters=getOption("java.parameters"))
}

.rhive.connect <- function(host="127.0.0.1", port=10000, hiveServer2=NA, defaultFS=NULL, updateJar=FALSE, user=NULL, password=NULL) {

  initialized <- .getEnv("INITIALIZED")
  if (is.null(.getEnv("HIVE_HOME")) || is.null(.getEnv("HADOOP_HOME"))) {
    warning(
              paste(
              "\n\t+-------------------------------------------------------------------------------+\n",
                "\t+ / Can't find the environment variable 'HIVE_HOME' or 'HADOOP_HOME'.           +\n",
                "\t+ / Retry rhive.connect() after calling rhive.init() with proper arguments.     +\n",
                "\t+-------------------------------------------------------------------------------+\n", sep=""), call.=FALSE, immediate.=TRUE)
  } else if (is.null(initialized) || !initialized) {
    warning(
              paste(
              "\n\t+-------------------------------------------------------------------------------+\n",
                "\t+ / RHive not initialized properly.                                             +\n",
                "\t+ / Retry rhive.connect() after calling rhive.init() with proper arguments.     +\n",
                "\t+-------------------------------------------------------------------------------+\n", sep=""), call.=FALSE, immediate.=TRUE)

  } else {
    if (.isConnected()) {
     .rhive.close()
    }

    EnvUtils <- .j2r.EnvUtils()
    userName <- EnvUtils$getUserName()
    userHome <- EnvUtils$getUserHome()
    tmpDir <- EnvUtils$getTempDirectory()

   .setEnv("USERNAME", userName)
   .setEnv("HOME", userHome)
   .setEnv("TMP_DIR", tmpDir)

    System <- .j2r.System()
    System$setProperty("RHIVE_UDF_DIR", .FS_UDF_DIR())

    if (is.null(defaultFS)) {
      defaultFS <- .DEFAULT_FS()
    }
   .rhive.hdfs.connect(defaultFS)
   .copyJarsToHdfs(updateJar)

    if (is.na(hiveServer2)) {
      hiveServer2 <- .isForVersion2()
    }

    hiveClient <- .j2r.HiveJdbcClient(hiveServer2)
    hiveClient$connect(host, as.integer(port), user, password) 
    hiveClient$addJar(.FS_JAR_PATH())

   .registerUDFs(hiveClient)
   .setConfigurations(hiveClient)

   .setEnv("hiveClient", hiveClient)

   .makeBaseDirs()
  }
}

.copyJarsToHdfs <- function(updateJar) {
  jar <- paste(system.file(package="RHive"), "java", "rhive_udf.jar", sep=.Platform$file.sep)

  if (updateJar) {
   .rhive.hdfs.put(jar, .FS_JAR_PATH(), overwrite=TRUE)
  } else if (!.rhive.hdfs.exists(.FS_JAR_PATH())) {
   .rhive.hdfs.put(jar, .FS_JAR_PATH())
  }
}

.isForVersion2 <- function() {
  hiveServer2 <- NA

  ver <- .HIVESERVER_VERSION()
  if (!is.null(ver)) {
    verNum <- as.integer(ver)
    if (verNum == 1L) {
      hiveServer2 <- FALSE
    } else if (verNum == 2L) {
      hiveServer2 <- TRUE
    }
  }

  if (is.na(hiveServer2)) {
    hiveServer2 <- TRUE
    warning(
      paste(
        "\n\t+----------------------------------------------------------+\n",
          "\t+ / hiveServer2 argument has not been provided correctly.  +\n",
          "\t+ / RHive will use a default value: hiveServer2=TRUE.      +\n",
          "\t+----------------------------------------------------------+\n", sep=""), call.=FALSE, immediate.=TRUE)
  }

  return (hiveServer2)
}

.registerUDFs <- function(hiveClient) {
  hiveClient$execute(sprintf("CREATE TEMPORARY FUNCTION %s AS \"%s\"", "R", "com.nexr.rhive.hive.udf.RUDF"))
  hiveClient$execute(sprintf("CREATE TEMPORARY FUNCTION %s AS \"%s\"", "RA", "com.nexr.rhive.hive.udf.RUDAF"))
  hiveClient$execute(sprintf("CREATE TEMPORARY FUNCTION %s AS \"%s\"", "unfold", "com.nexr.rhive.hive.udf.GenericUDTFUnFold"))
  hiveClient$execute(sprintf("CREATE TEMPORARY FUNCTION %s AS \"%s\"", "expand", "com.nexr.rhive.hive.udf.GenericUDTFExpand"))
  hiveClient$execute(sprintf("CREATE TEMPORARY FUNCTION %s AS \"%s\"", "rkey", "com.nexr.rhive.hive.udf.RangeKeyUDF"))
  hiveClient$execute(sprintf("CREATE TEMPORARY FUNCTION %s AS \"%s\"", "scale", "com.nexr.rhive.hive.udf.ScaleUDF"))
  hiveClient$execute(sprintf("CREATE TEMPORARY FUNCTION %s AS \"%s\"", "array2String", "com.nexr.rhive.hive.udf.GenericUDFArrayToString"))
}

.setConfigurations <- function(hiveClient) {
 .rhive.set(.HIVE_OUTPUT_COMPRESSION_PROPERTY, "false", hiveClient) 
 .rhive.set(.HADOOP_CHILD_ENV_PROPERTY, sprintf("RHIVE_UDF_DIR=%s", .FS_UDF_DIR()), hiveClient)
}

.makeBaseDirs <- function() {
  if (!.rhive.hdfs.exists(.FS_BASE_DATA_DIR())) {
    .dfs.mkdir(.FS_BASE_DATA_DIR())
    .dfs.chmod("777", .FS_BASE_DATA_DIR())
  }

  if (!.rhive.hdfs.exists(.FS_BASE_UDF_DIR())) {
    .dfs.mkdir(.FS_BASE_UDF_DIR())
    .dfs.chmod("777", .FS_BASE_UDF_DIR())
  }

  if (!.rhive.hdfs.exists(.FS_BASE_TMP_DIR())) {
    .dfs.mkdir(.FS_BASE_TMP_DIR())
    .dfs.chmod("777", .FS_BASE_TMP_DIR())
  }

  if (!.rhive.hdfs.exists(.FS_BASE_MR_SCRIPT_DIR())) {
    .dfs.mkdir(.FS_BASE_MR_SCRIPT_DIR())
    .dfs.chmod("777", .FS_BASE_MR_SCRIPT_DIR())
  }
}

.getHiveClient <- function() {
  hiveClient <- .getEnv("hiveClient")
 .checkConnection(hiveClient)

  return (hiveClient)
}

.isConnected <- function() {
  hiveClient <- .getEnv("hiveClient")
  if (!is.null(hiveClient)) {
    tryCatch ( {
        hiveClient$checkConnection()  
      }, error=function(e) { 
        return (FALSE)
      }
    )
  } else {
    return (FALSE)
  }

  return (TRUE)
}


.rhive.close <- function() {
 .rhive.hive.close()
 .rhive.hdfs.close()
  return (TRUE)
}

.rhive.hive.close <- function() {
  hiveClient <- .getHiveClient()
  if (!is.null(hiveClient)) {
    hiveClient$close()
  }

 .unsetEnv("hiveClient")
  return (TRUE)
}



.rhive.big.query <- function(query, fetchSize=50, limit=-1, memLimit=64*1024*1024) {
  table <- .HIVE_QUERY_RESULT_TABLE_NAME()
 .createTableAs(table, query)
  size <- .rhive.size.table(table)

  if (size > memLimit) {
    x <- table
    attr(x, "result:size") <- size
    return(x)

  } else {
    if (size < 2*1024*1024) {
      result <- .getData(table, fetchSize, limit)
    } else {
      result <- .rhive.load.table2(table)
    }

   .rhive.drop.table(table)
    return(result)
  }
}

.createTableAs <- function(table, query) {
  query <- sprintf("CREATE TABLE %s AS %s", table, query)
 .rhive.execute(query)
}


.rhive.query <- function(query, fetchSize=50, limit=-1) {
  hiveClient <- .getHiveClient()

  lst <- list()
  result <- hiveClient$query(query, as.integer(limit), as.integer(fetchSize))

  colNames <- result$getColumns()
  colTypes <- result$getColumnTypes()

  if (length(colTypes) > 0) {
    for (i in seq.int(length(colTypes))) {
      colType <- colTypes[i]
      colName <- colNames[i]
      if (colType == "string") {
        lst[[i]] <- character()
      } else if (length(grep("^array", colType)) > 0) {
        lst[[i]] <- character()
      } else {
        lst[[i]] <- numeric()
      }
      names(lst)[i] <- colName
    }
  }

  rec <- result$getNext() 
  while (!is.null(rec)) {
    for (i in seq.int(length(rec))) {
      if (is.numeric(lst[[i]])) {
        lst[[i]] <- c(lst[[i]], as.numeric(rec[i]))
      } else {
        lst[[i]] <- c(lst[[i]], rec[i])
      }
    } 

    if (length(lst) > length(rec)) {
      for (i in seq.int(length(rec) + 1, length(lst))) {
        if (is.numeric(lst[[i]])) {
          lst[[i]] <- c(lst[[i]], NA)
        } else {
          lst[[i]] <- c(lst[[i]], "")
        }
      }
    }

    rec <- result$getNext()
  }

  result$close()
  return(as.data.frame(lst))
}

.rhive.execute <- function(query) {
  hiveClient <- .getHiveClient()
  hiveClient$execute(query)
  return (TRUE)
}

.rhive.assign.export <- function(name, value) {
  assign(name, value, envir=.rhiveExportEnv)
  return(TRUE)
}

.rhive.rm.export <- function(name) {
  rm(list=name, envir=.rhiveExportEnv)
  return(TRUE)
}

.rhive.assign <- .rhive.assign.export
.rhive.rm <- .rhive.rm.export

.rhive.export <- function(exportName, pos=-1, limit=100*1024*1024, ALL=FALSE) {
  dataPath <- .TMP_FILE(exportName)
  if (!ALL) {
    if (object.size(get(exportName, pos, envir=.rhiveExportEnv)) > limit) {
      print("Object size limit exceeded")
      return (FALSE)
    }

    cmd <-  sprintf("save(%s, file=\"%s\", envir=.rhiveExportEnv)", exportName, dataPath)
  } else {
    if (attr(.rhiveExportEnv, "name") <- "no attribute" == "package:RHive") {
      print("Can't export 'package:RHive'")
      return(FALSE)
    }

    list <- ls(NULL, pos, envir=.rhiveExportEnv)

    total_size <- 0
    for (item in list) {
      value <- get(item, pos, envir=.rhiveExportEnv)
      total_size <- total_size + object.size(value)
      if (total_size > limit) {
        print("Object size limit exceeded")
        return (FALSE)
      }
    }

    cmd <- sprintf("save(list=ls(pattern=\"[^exportName]\", envir=.rhiveExportEnv), file=\"%s\", envir=.rhiveExportEnv)", dataPath)
  }

  eval(parse(text=cmd))

  UDFUtils <- .j2r.UDFUtils()
  UDFUtils$export(exportName, dataPath)

  return(TRUE)
}

.rhive.exportAll <- function(exportName, pos=1, limit=100*1024*1024) {
 .rhive.export(exportName, pos, limit, ALL=TRUE) 
}


.rhive.list.udfs <- function() {
  UDFUtils <- .j2r.UDFUtils()
  return (UDFUtils$list())
}

.rhive.rm.udf <- function(exportName) {
  UDFUtils <- .j2r.UDFUtils()
  return (UDFUtils$delete(exportName))
}

.rhive.list.databases <- function(pattern) {
  tableList <- .rhive.query("SHOW DATABASES")
  all.names <- as.character(tableList[,'database_name'])
  
  if (!missing(pattern)) {
    if ((ll <- length(grep("[", pattern, fixed=TRUE))) &&
         ll != length(grep("]", pattern, fixed=TRUE))) {
      if (pattern == "[") {
        pattern <- "\\["
        warning(
              paste(
              "\n\t+-------------------------------------------------------------+\n",
                "\t+ / Replaced '[' by  '\\\\[' in regular expression pattern.   +\n",
                "\t+-------------------------------------------------------------+\n", sep=""), call.=FALSE, immediate.=TRUE)

      } else if (length(grep("[^\\\\]\\[<-", pattern))) {
        pattern <- sub("\\[<-", "\\\\\\[<-", pattern)
        warning(
              paste(
              "\n\t+----------------------------------------------------------------+\n",
                "\t+ / Replaced '[<-' by '\\\\[<-' in regular expression pattern.   +\n",
                "\t+---------------------------------------------------------i------+\n", sep=""), call.=FALSE, immediate.=TRUE)
      }
    }

    all.names <- grep(pattern, all.names, value=TRUE)
  }
  
  databaseName <- all.names
  return(as.data.frame(databaseName))
}

.rhive.show.databases <- .rhive.list.databases

.rhive.use.database <- function(databaseName="default") {
 .rhive.execute(sprintf("USE %s", databaseName))
}

.rhive.list.tables <- function(pattern) {
  tableList <- .rhive.query("SHOW TABLES")
  all.names <- as.character(tableList[,'tab_name'])
  if (!missing(pattern)) {
    if ((ll <- length(grep("[", pattern, fixed=TRUE))) && ll != length(grep("]", pattern, fixed=TRUE))) {
      if (pattern == "[") {
        pattern <- "\\["
        warning(
              paste(
              "\n\t+-------------------------------------------------------------+\n",
                "\t+ / Replaced '[' by  '\\\\[' in regular expression pattern.   +\n",
                "\t+-------------------------------------------------------------+\n", sep=""), call.=FALSE, immediate.=TRUE)
      }

      else if (length(grep("[^\\\\]\\[<-", pattern))) {
        pattern <- sub("\\[<-", "\\\\\\[<-", pattern)
        warning(
              paste(
              "\n\t+----------------------------------------------------------------+\n",
                "\t+ / Replaced '[<-' by '\\\\[<-' in regular expression pattern.   +\n",
                "\t+---------------------------------------------------------i------+\n", sep=""), call.=FALSE, immediate.=TRUE)
      }
    }

    all.names <- grep(pattern, all.names, value=TRUE)
  }

  tab_name <- all.names
  return(as.data.frame(tab_name))
}

.rhive.show.tables <- .rhive.list.tables

.rhive.desc.table <- function(tableName, detail=FALSE) {
  if (!is.character(tableName)) {
    stop("tableName must be string type.")
  }

  res <- NULL
  if (detail) {
    tableInfo <- .rhive.query(paste("DESCRIBE EXTENDED",tableName))
    res <- tableInfo[[2]][length(rownames(tableInfo))]
  } else {
    res <- .rhive.query(paste("DESCRIBE", tableName))
  }

  if (!is.null(res)) {
    res <- lapply(res, function(v) { gsub("(^ +)|( +$)", "", v) })
  }

  return (as.data.frame(res))
}

.rhive.load.table <- function(tableName, fetchSize=50, limit=-1) {
  memSize <- 2*1024*1024

  if (!is.character(tableName)) {
    stop("tableName must be string type.")
  }

  size <- .rhive.size.table(tableName)

  if (size <= memSize) {
    result <- .getData(tableName, fetchSize, limit)
    return (result)
  } else {
    result <- .rhive.load.table2(tableName, limit)
    return (result)
  }
}

.rhive.load.table2 <- function(tableName, limit=-1, remote=TRUE) {
  colnames <- NULL

  localDir <- .WORKING_DIR(sub=TRUE)
  if (!file.exists(localDir)) {
    dir.create(localDir, recursive=TRUE)
  }
  Sys.chmod(localDir, mode="777", use_umask=FALSE) 

  if (remote) {
    hdfsDir <- .FS_TMP_DIR(sub=TRUE)

    if (!.rhive.hdfs.exists(hdfsDir)) {
     .dfs.mkdir(hdfsDir)
    }

    colnames <- .rhive.query(sprintf("INSERT OVERWRITE DIRECTORY \"%s\" SELECT * FROM %s", hdfsDir, tableName))

   .rhive.hdfs.get(hdfsDir, localDir, srcDel=FALSE);
   .dfs.rm(hdfsDir)
  } else {
    colnames <- .rhive.query(sprintf("INSERT OVERWRITE LOCAL DIRECTORY \"%s\" SELECT * FROM %s", localDir, tableName))
  }

  fullData <- NULL
  for (filename in list.files(localDir, full.names=TRUE, recursive=TRUE)) {
    data <- read.csv(file=filename, header=FALSE, sep='\001', nrows=limit)
    if (is.null(fullData)) {
      fullData <- data
    } else {
      fullData <- rbind(fullData, data)
    }
  }

  if (!is.null(fullData)) {
    names(fullData) <- names(colnames)
    unlink(localDir, recursive=TRUE, force=TRUE)
  }

  return(fullData)
}

.rhive.exist.table <- function(tableName) {
  if (!is.character(tableName)) {
    stop("tableName must be string type.")
  }

  tableList <- .rhive.list.tables()
  if (length(row.names(tableList)) == 0) {
    return(FALSE)
  }

  loc <- try((tableList == tolower(tableName)), silent=TRUE)
  if (class(loc) == "try-error") {
    return(FALSE)
  }

  if (length(tableList[loc]) == 0) {
    return(FALSE)
  } else {
    return(TRUE)
  }
}

.rhive.set <- function(key, value, hiveClient=.getHiveClient()) {
  if (missing(key) && missing(value)) {
    return (hiveClient$listConfigs(TRUE))
  } else {
    return (hiveClient$set(key, value))
  }
}

.rhive.unset <- function(key, hiveClient=.getHiveClient()) {
  return (hiveClient$set(key, ""))
}

.rhive.napply <- function(tableName, FUN, ..., forcedRef=TRUE) {
  if (!is.character(tableName)) {
    stop("tableName must be string type.")
  }

  exportName <- .NAPPLY_NAME()
 .rhive.assign.export(exportName, FUN)
 .rhive.export(exportName, ALL=TRUE)

  if (length(c(...)) > 0) {
    cols <- paste(c(...), collapse=",")
    hql <- sprintf("SELECT R(\"%s\", %s, 0.0) FROM %s", exportName, cols, tableName)
  } else {
    hql <- sprintf("SELECT R(\"%s\", 0.0) FROM %s", exportName, tableName)
  }

  if (forcedRef) {
    result <- .rhive.big.query(hql, memLimit=-1)
  } else {
    result <- .rhive.big.query(hql)
  }

  return(result)
}

.rhive.sapply <- function(tableName, FUN, ..., forcedRef=TRUE) {
  if (!is.character(tableName)) {
    stop("tableName must be string type.")
  }

  exportName <- .SAPPLY_NAME()
 .rhive.assign.export(exportName, FUN)
 .rhive.export(exportName, ALL=TRUE)

  if (length(c(...)) > 0) {
    cols <- paste(c(...), collapse=",")
    hql <- sprintf("SELECT R(\"%s\", %s, \"\") FROM %s", exportName, cols, tableName)
  } else {
    hql <- sprintf("SELECT R(\"%s\", \"\") FROM %s", exportName, tableName)
  }

  if (forcedRef) {
    result <- .rhive.big.query(hql, memLimit=-1)
  } else {
    result <- .rhive.big.query(hql)
  }

  return(result)
}

.rhive.aggregate <- function(tableName, hiveFUN, ..., groups=NULL , forcedRef=TRUE) {
  if (!is.character(tableName)) {
    stop("tableName must be string type.")
  }

  cols <- paste(c(...), collapse=",")
  if (is.null(groups)) {
    hql <- sprintf("SELECT %s(%s) FROM %s", hiveFUN , cols, tableName)
  } else {
    groupBy <- paste(groups, collapse=",")
    hql <- sprintf("SELECT %s(%s) FROM %s GROUP BY %s", hiveFUN , cols, tableName, groupBy)
  }

  if (forcedRef) {
    result <- .rhive.big.query(hql, memLimit=-1)
  } else {
    result <- .rhive.big.query(hql)
  }

  return(result)
}

.rhive.mapapply <- function(tableName, mapperFUN, mapInput=NULL, mapOutput=NULL, by=NULL, args=NULL, bufferSize=-1L, verbose=FALSE, forcedRef=TRUE) {

 .rhive.mrapply(tableName, 
                mapperFUN=mapperFUN, 
                reducerFUN = NULL,
                mapInput=mapInput,
                mapOutput=mapOutput, 
                by=by,
                mapperArgs=args, 
                reducerArgs=NULL, 
                bufferSize=bufferSize,
                verbose=verbose,
                forcedRef = forcedRef)
}

.rhive.reduceapply <- function(tableName, reducerFUN, reduceInput=NULL,reduceOutput=NULL, args=NULL, bufferSize=-1L, verbose=FALSE, forcedRef=TRUE) {

 .rhive.mrapply(tableName, 
                mapperFUN=NULL, 
                reducerFUN=reducerFUN, 
                reduceInput=reduceInput,
                reduceOutput=reduceOutput,
                mapperArgs=NULL,
                reducerArgs=args,
                bufferSize=bufferSize,
                verbose=verbose,forcedRef=forcedRef)
}

.rhive.mrapply <- function(tableName, mapperFUN, reducerFUN, mapInput=NULL, mapOutput=NULL, by=NULL, reduceInput=NULL,reduceOutput=NULL, mapperArgs=NULL, reducerArgs=NULL, bufferSize=-1L, verbose=FALSE, forcedRef=TRUE) {

  if (!is.character(tableName)) {
    stop("tableName must be string type.")
  }

  exportName <- .MRAPPLY_NAME()
 .rhive.script.export(exportName, mapperFUN, reducerFUN, mapperArgs, reducerArgs, bufferSize=bufferSize)

  mapperScript <- NULL
  if (!is.null(mapperFUN)) {
    mapperScript <- .MAPPER_SCRIPT_NAME(exportName)
  }

  reducerScript <- NULL
  if (!is.null(reducerFUN)) {
    reducerScript <- .REDUCER_SCRIPT_NAME(exportName)
  }

  miCols <- "*"
  moCols <- NULL
  if (!is.null(mapInput)) {
    miCols <- paste(mapInput, collapse=",")
    moCols <- miCols
  }

  if (!is.null(mapOutput)) {
    moCols <- paste(mapOutput, collapse=",")
  }

  riCols <- "*"
  roCols <- NULL
  if (!is.null(reduceInput)) {
    if (is.null(mapperFUN)) {
      riCols <- paste(reduceInput, collapse=",")
    } else {
      riCols <- paste(sprintf("MAPOUTPUT.%s", reduceInput), collapse=",")
    }
  }

  if (!is.null(reduceOutput)) {
    roCols <- paste(reduceOutput, collapse=",")
  }

  if (is.null(mapperFUN)) {
    hql <- sprintf("FROM %s", tableName)
  } else {
    if (is.function(mapperFUN)) {
      hql <- sprintf("FROM (FROM %s MAP %s USING \"%s\"", tableName, miCols, mapperScript)

      if (!is.null(moCols)) {
        hql <- sprintf("%s AS %s", hql, moCols)
      }

      if (is.null(by)) {
        hql <- sprintf("%s) MAPOUTPUT", hql)
      } else {
        hql <- sprintf("%s CLUSTER BY %s) MAPOUTPUT", hql, by)
      }
    } else {
      hql <- sprintf("FROM ( %s ) MAPOUTPUT", mapperFUN)
    }
  }

  isBigQuery <- FALSE
  tmpTable <- NULL

  if (!is.null(reducerFUN)) {
    tmpTable <- .HIVE_QUERY_RESULT_TABLE_NAME()
    
    q <- NULL
    if (!is.null(reduceOutput)) {
      q <- sprintf("CREATE TABLE %s ( %s )", tmpTable, paste(reduceOutput, "string", collapse=","))
    } 

    if (is.null(q)) {
      stop("Failed to generate create query because no reduce-output column is provided.")
    }
      
   .rhive.execute(q)

    hql <- sprintf("%s INSERT OVERWRITE TABLE %s SELECT TRANSFORM ( %s ) USING \"%s\"", hql, tmpTable, riCols, reducerScript)
    if (!is.null(roCols)) {
      hql <- sprintf("%s AS %s", hql, roCols)
    }
  } else {
    if (is.null(moCols)) {
      hql <- sprintf("SELECT * %s", hql)
    } else {
      hql <- sprintf("SELECT %s %s", moCols, hql)
    }

    isBigQuery <- TRUE
  }

  if (is.null(mapperFUN) && is.null(reducerFUN)) {
    hql <- sprintf("SELECT * FROM %s", tableName)
    isBigQuery <- TRUE
  }
    
  if (verbose) {
    sprintf("HIVE-QUERY : %s", hql)
  }
  
  cacheConfig <- .getCacheConfig(.FS_MR_SCRIPT_DIR(), mapperScript, reducerScript)
  if (!is.null(cacheConfig)) {
   .rhive.set(.HIVE_CACHE_FILES_PROPERTY, cacheConfig)
    on.exit(.rhive.unset(.HIVE_CACHE_FILES_PROPERTY))
  }
 
  resultSet <- NULL
  if (isBigQuery) {
    if (forcedRef) {
      resultSet <- .rhive.big.query(hql, memLimit=-1)
    } else {
      resultSet <- .rhive.big.query(hql)
    }
  } else {
    .rhive.execute(hql)
    length <- .rhive.size.table(tmpTable)

    memSize <- 64*1024*1024
    if (forcedRef || length > memSize) { 
      x <- tmpTable
      attr(x, "result:size") <- length
      resultSet <- x
    } else {
      result <- .rhive.query(sprintf("SELECT * FROM %s", tmpTable))
     .rhive.drop.table(tmpTable)
      resultSet <- result
    }
  }

 .rhive.script.unexport(exportName)
  return(resultSet)
}

.getCacheConfig <- function(dir, ...) {
  defaultFS <- .DEFAULT_FS()

  files <- c(...) 
  v <- sapply(files, FUN=function(file) { sprintf("%s%s/%s#%s", sub("\\/$", "", defaultFS), dir, file, file) })
  return (paste(v, collapse=","))
}



.rhive.drop.table <- function(tableName, list) {
  if (!missing(tableName)) {     
    tableName <- tolower(tableName)
   .rhive.execute(sprintf("DROP TABLE IF EXISTS %s", tableName))
  }

  if (!missing(list)) {
    if (is.data.frame(list)) {
      list <- as.character(list[,'tab_name'])
    }
    
    for (tableName in list) {        
      tableName <- tolower(tableName)
     .rhive.execute(sprintf("DROP TABLE IF EXISTS %s", tableName))    
    }
  }
}

.rhive.size.table <- function(tableName) {
  if (missing(tableName)) {
      stop("missing tableName")
  }

  tableName <- tolower(tableName)
  metaInfo <- .rhive.desc.table(tableName, detail=TRUE)
  location <- strsplit(strsplit(as.character(metaInfo[[1]]), "location:")[[1]][2],",")[[1]][1]
  dataInfo <- .rhive.hdfs.du(location, summary=TRUE)

  return (dataInfo$length)
}

.rhive.list.jobs <- function() {
  jobManager <- .j2r.JobManager()
  jobManager$listJobs()
}

.checkConnection <- function(hiveClient) {
  if (!is.null(hiveClient)) {
    hiveClient$checkConnection()  
  } else {
    stop("Not connected to HiveServer")
  }
}

.getData <- function(table, fetchSize=50, limit=-1) {
  if (limit > 0) {
    query <- sprintf("SELECT * FROM %s LIMIT %d", table, limit) 
  } else {
    query <- sprintf("SELECT * FROM %s", table) 
  }

 .rhive.query(query, fetchSize, limit)
}

.dfs.mkdir <- function(dir) {
  tryCatch ( {
     .rhive.execute(sprintf("dfs -mkdir %s", dir))
      return (TRUE)
    }, error=function(e) {
      return (FALSE)
    }
  )
}

.dfs.rm <- function(file) {
  tryCatch ( {
     .rhive.execute(sprintf("dfs -rmr -skipTrash %s", file))
      return (TRUE)
    }, error=function(e) {
      return (FALSE)
    }
  ) 
}

.dfs.chmod <- function(mod, file) {
  tryCatch ( {
     .rhive.execute(sprintf("dfs -chmod %s %s", mod, file))
      return (TRUE)
    }, error=function(e) {
      return (FALSE)
    }
  )
}
