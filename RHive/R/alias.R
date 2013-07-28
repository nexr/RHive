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


hiveConnect <- function(host="127.0.0.1", port=10000, hiveServer2=NA, defaultFS=NULL, updateJar=FALSE) {
  .rhive.connect(host, port, hiveServer2, defaultFS, updateJar)
}

hiveClose <- function() {
  .rhive.close()
}

hiveQuery <- function(query, fetchSize=50, limit=-1) {
  .rhive.query(query, fetchSize, limit)
}

hiveExport <- function(exportName, pos=-1, limit=100*1024*1024, ALL=FALSE) {
  .rhive.export(exportName, pos, limit, ALL)
}

hiveExportAll <- function(exportName, pos=1, limit=100*1024*1024) {
  .rhive.exportAll(exportName, pos, limit)
}

hiveListDatabases <- function(pattern) {
  .rhive.list.databases(pattern)
}

hiveShowDatabases <- function(pattern) {
  .rhive.show.databases(pattern)
}

hiveUseDatabase <- function(databaseName) {
  .rhive.use.database(databaseName)
}

hiveListTables <- function(pattern) {
  .rhive.list.tables(pattern)
}

hiveShowTables <- function(pattern) {
  .rhive.list.tables(pattern)
}

hiveDescTable <- function(tableName, detail=FALSE) {
  .rhive.desc.table(tableName=tableName, detail=detail)
}

hiveLoadTable <- function(tableName, fetchSize=50, limit=-1) {
  .rhive.load.table(tableName, fetchSize, limit) 
}

hiveAssign <- function(name, value) {
  .rhive.assign(name, value)
}

hiveRm <- function(name) {
  .rhive.rm(name)
}
