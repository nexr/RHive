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
    .rhiveEnv[[name]]
  }
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

  if (!is.null(.getEnv("USERNAME"))) {
    cat(sprintf("\n\tuser name: %s", .getEnv("USERNAME")))
  }
  if (!is.null(.getEnv("HOME"))) {
    cat(sprintf("\n\tuser home: %s", .getEnv("HOME")))
  }
  if (!is.null(.getEnv("TMP_DIR"))) {
    cat(sprintf("\n\ttemp dir: %s", .getEnv("TMP_DIR")))
  }
 
  if (ALL) {
    classpath <- .getEnv("CLASSPATH")
    cat("\n\tclasspath:")
    cat(sprintf("\n\t\t%s", classpath))
    cat("\n")
  }
}
