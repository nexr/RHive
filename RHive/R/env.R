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



.getSysEnv <- function(name) {
  value <- Sys.getenv(name)
  if (!is.na(value) && !is.null(value)) {
    v <- as.character(value)
    if (nchar(v) > 0) {
      return (v)
    }
  }

  return (NULL)
}

.setEnv <- function(name, value) {
  assign(name, value, envir=.rhiveEnv)
}

.unsetEnv <- function(name) {
  value <- .getEnv(name) 
  if (!is.null(value)) {
    rm(list=name, envir=.rhiveEnv)
  }
}

.getEnv <- function(name) {
  if (missing(name)) {
    as.list(.rhiveEnv)
  } else {
    RHive:::.rhiveEnv[[name]]
  }
}

.initEnv <- function(hiveHome=NULL, hiveLib=NULL, hadoopHome=NULL, hadoopConf=NULL, hadoopLib=NULL, verbose=FALSE) {

  if (is.null(hiveHome)) {
    hiveHome <- .getSysEnv("HIVE_HOME")
  }

  if (is.null(hiveHome)) {
    stop("Can't find the environment variable 'HIVE_HOME'.\nMake sure it is set correctly.")
  }

  if (is.null(hadoopHome)) {
    hadoopHome <- .getSysEnv("HADOOP_HOME")
  }

  if (is.null(hadoopHome)) {
    stop("Can't find the environment variable 'HADOOP_HOME'.\nMake sure it is set correctly.")
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

  if (is.null(hadoopConf)) {
    hadoopConf <- .getSysEnv("HADOOP_CONF_DIR")
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
 .jinit(classpath=cp, parameters=.getJavaParameters())

  EnvUtils <- .j2r.EnvUtils()
  userName <- EnvUtils$getUserName()
  userHome <- EnvUtils$getUserHome()
  tmpDir <- EnvUtils$getTempDirectory()

 .setEnv("CLASSPATH", cp)
 .setEnv("USERNAME", userName)
 .setEnv("HOME", userHome)
 .setEnv("TMP_DIR", tmpDir)

  if (verbose) {
    .rhive.env(ALL=TRUE)
  }
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

  if (substr(hiveLib, start=1, stop=nchar(hiveHome)) != hiveHome) {
    cp <- c(cp, list.files(hiveLib, full.names=TRUE, pattern="jar$", recursive=TRUE))
  }

  cp <- c(cp, list.files(hiveHome, full.names=TRUE, pattern="jar$", recursive=TRUE), hadoopConf)

  return (cp)
}

.getJavaParameters <- function() {
  return (getOption("java.parameters"))
}

.rhive.env <- function(ALL=FALSE) {
  cat(sprintf("\thadoop home: %s", .getEnv("HADOOP_HOME")))
  cat(sprintf("\n\thadoop conf: %s", .getEnv("HADOOP_CONF_DIR")))

  defaultFS <- .getEnv("DEFAULT_FS")
  if (!is.null(defaultFS)) {
    cat(sprintf("\n\tfs: %s", defaultFS))
  }

  if (!is.null(.getEnv("FS_HOME"))) { 
    cat(sprintf("\n\tfs home: %s", .getEnv("FS_HOME")))
  }

  cat(sprintf("\n\thive home: %s", .getEnv("HIVE_HOME")))
  cat(sprintf("\n\thive lib: %s", .getEnv("HIVE_LIB")))
  if (!is.null(.getEnv("HIVESERVER_VERSION"))) {
    cat(sprintf("\n\thiveserver version: %s", .getEnv("HIVESERVER_VERSION")))
  }

  cat(sprintf("\n\tuser name: %s", .getEnv("USERNAME")))
  cat(sprintf("\n\tuser home: %s", .getEnv("HOME")))
  cat(sprintf("\n\ttemp dir: %s", .getEnv("TMP_DIR")))
 
  if (ALL) {
    classpath <- .getEnv("CLASSPATH")
    cat("\n\tclasspath:")
    cat(sprintf("\n\t\t%s", classpath))
    cat("\n")
  }
}
