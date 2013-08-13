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




.HIVE_OUTPUT_COMPRESSION_PROPERTY <- "hive.exec.compress.output"

.HIVE_CACHE_FILES_PROPERTY <- "mapred.cache.files"

.HADOOP_DEFAULT_FS_PROPERTY <- "fs.default.name"

.NEW_HADOOP_DEFAULT_FS_PROPERTY <- "fs.defaultFS"

.HADOOP_CHILD_ENV_PROPERTY <- "mapred.child.env"

.RDATA_FILE_EXT <- ".RData"

.DEFAULT_FS_HOME <- "/rhive"



.USERNAME <- function() {
  .getEnv("USERNAME")
}

.HOME <- function() {
  .getEnv("HOME")
}


.CLASSPATH <- function() {
  .getEnv("CLASSPATH")
}

.HIVE_HOME <- function() {
  .getEnv("HIVE_HOME")
}

.HADOOP_HOME <- function() {
  .getEnv("HADOOP_HOME")
}

.HIVESERVER_VERSION <- function() {
  .getEnv("HIVESERVER_VERSION")
}

.FS_HOME <- function() {
  fsHome <- .getEnv("FS_HOME")
  if (is.null(fsHome)) {
    fsHome <- .DEFAULT_FS_HOME
  }

  return (fsHome)
}

.HIVE_LIB_DIR <- function() {
  dir <- .getEnv("HIVE_LIB_DIR") 
  if (!is.null(dir)) {
    return (dir)
  }

  return (.defaultLibDir(.HIVE_HOME()))
}


.HADOOP_LIB_DIR <- function() {
  dir <- .getEnv("HADOOP_LIB_DIR") 
  if (!is.null(dir)) {
    return (dir)
  }

  return (.defaultLibDir(.HADOOP_HOME()))
}

.HADOOP_CONF_DIR <- function() {
  dir <- .getEnv("HADOOP_CONF_DIR") 
  if (!is.null(dir)) {
    return (dir)
  }

  return (.defaultConfDir(.HADOOP_HOME())) 
}

.TMP_DIR <- function(sub=FALSE) {
  if (sub) {
    return (sprintf("%s/%s", .getEnv("TMP_DIR"), .makeRandomKey()))
  } else {
    return (.getEnv("TMP_DIR"))
  }
}

.TMP_FILE <- function(name, sub=FALSE) {
  if (!file.exists(.TMP_DIR())) {
    dir.create(.TMP_DIR(), recursive=TRUE)
  }

  if (sub) {
    return (sprintf("%s/%s_%s", .TMP_DIR(), name, .makeRandomKey()))
  } else {
    return (sprintf("%s/%s", .TMP_DIR(), name))
  }
}

.WORKING_DIR <- function(name, sub=FALSE) {
  if (sub) {
    return (sprintf("%s/%s", getwd(), .makeRandomKey()))
  } else {
    return (getwd())
  }
}

.DEFAULT_FS <- function() {
  fs <- .getEnv("DEFAULT_FS") 
  if (!is.null(fs)) {
    return (fs)
  }

  config <- .j2r.Configuration()
  return (config$get(.HADOOP_DEFAULT_FS_PROPERTY))
}

.FS_BASE_UDF_DIR <- function() {
  return (sprintf("%s/udf", .FS_HOME()))
}

.FS_UDF_DIR <- function() {
  return (sprintf("%s/%s", .FS_BASE_UDF_DIR(), .USERNAME()))
}

.FS_UDF_FILE <- function(name) {
  return (sprintf("%s/%s", .FS_UDF_DIR(), name))
}

.FS_JAR_PATH <- function() {
  return (sprintf("%s%s/lib/%s/rhive_udf.jar", sub("\\/$", "", .DEFAULT_FS()), .FS_HOME(), installed.packages()["RHive", "Version"]))
}

.FS_BASE_TMP_DIR <- function() {
  return (sprintf("%s/tmp", .FS_HOME()))
}

.FS_TMP_DIR <- function(sub=FALSE) {
  if (sub) {
    return (sprintf("%s/%s/%s", .FS_BASE_TMP_DIR(), .USERNAME(), .makeRandomKey()))
  } else {
    return (sprintf("%s/%s", .FS_BASE_TMP_DIR(), .USERNAME()))
  }
}

.FS_TMP_FILE <- function(name, sub=FALSE) {
  if (sub) {
    return (sprintf("%s/%s", .FS_TMP_DIR(sub), name))
  } else {
    return (sprintf("%s/%s_%s", .FS_TMP_DIR(sub), name, .makeRandomKey()))
  }
}

.FS_BASE_DATA_DIR <- function() {
  return (sprintf("%s/data", .FS_HOME()))
}

.FS_DATA_DIR <- function(sub=FALSE) {
  if (sub) {
    return (sprintf("%s/%s/%s", .FS_BASE_DATA_DIR(), .USERNAME(), .makeRandomKey()))
  } else {
    return (sprintf("%s/%s", .FS_BASE_DATA_DIR(), .USERNAME()))
  }
}

.FS_DATA_FILE <- function(name, sub=FALSE) {
  return (sprintf("%s/%s", .FS_DATA_DIR(sub), name))
}

.HIVE_QUERY_RESULT_TABLE_NAME <- function() {
  return (sprintf("RHIVE_RESULT_%s_%s", format(as.POSIXlt(Sys.time()),format="%Y%m%d%H%M%S"), .makeRandomKey(4)))
}

.FS_BASE_MR_SCRIPT_DIR <- function() {
  return (sprintf("%s/script", .FS_HOME()))
}

.FS_MR_SCRIPT_DIR <- function() {
  return (sprintf("%s/%s", .FS_BASE_MR_SCRIPT_DIR(), .USERNAME()))
}

.FS_MAPPER_SCRIPT_PATH <- function(name) {
  return (sprintf("%s/%s.mapper", .FS_MR_SCRIPT_DIR(), name))
}

.FS_REDUCER_SCRIPT_PATH <- function(name) {
  return (sprintf("%s/%s.reducer", .FS_MR_SCRIPT_DIR(), name))
}

.NAPPLY_NAME <- function() {
  sprintf("napply_%s", .makeRandomKey())
}

.SAPPLY_NAME <- function() {
  sprintf("sapply_%s", .makeRandomKey())
}

.MRAPPLY_NAME <- function() {
  sprintf("mrapply_%s", .makeRandomKey())
}

.MAPPER_SCRIPT_NAME <- function(mrapply) {
  sprintf("%s.mapper", mrapply)
}

.REDUCER_SCRIPT_NAME <- function(mrapply) {
  sprintf("%s.reducer", mrapply)
}


.defaultLibDir <- function(home) {
  return (sprintf("%s/lib", home))
}

.defaultConfDir <- function(home) {
  return (sprintf("%s/conf", home))
}

.makeRandomKey <- function(size=30) {
 k <- paste(sample(c(letters[1:6], 0:9), size, replace=TRUE), collapse="")
 return (k)
}
