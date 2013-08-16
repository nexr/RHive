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
  .rhive.connect(host=host, port=port, hiveServer2=hiveServer2, defaultFS=defaultFS, updateJar=updateJar)
}

hiveClose <- function() {
  .rhive.close()
}

hiveQuery <- function(query, fetchSize=50, limit=-1) {
  .rhive.query(query=query, fetchSize=fetchSize, limit=limit)
}

hiveExport <- function(exportName, pos=-1, limit=100*1024*1024, ALL=FALSE) {
  .rhive.export(exportName=exportName, pos=pos, limit=limit, ALL=ALL)
}

hiveExportAll <- function(exportName, pos=1, limit=100*1024*1024) {
  .rhive.exportAll(exportName=exportName, pos=pos, limit=limit)
}

hiveListDatabases <- function(pattern) {
  .rhive.list.databases(pattern=pattern)
}

hiveShowDatabases <- function(pattern) {
  .rhive.show.databases(pattern=pattern)
}

hiveUseDatabase <- function(databaseName) {
  .rhive.use.database(databaseName=databaseName)
}

hiveListTables <- function(pattern) {
  .rhive.list.tables(pattern=pattern)
}

hiveShowTables <- function(pattern) {
  .rhive.list.tables(pattern=pattern)
}

hiveDescTable <- function(tableName, detail=FALSE) {
  .rhive.desc.table(tableName=tableName, detail=detail)
}

hiveLoadTable <- function(tableName, fetchSize=50, limit=-1) {
  .rhive.load.table(tableName=tableName, fetchSize=fetchSize, limit=limit) 
}

hiveAssign <- function(name, value) {
  .rhive.assign(name=name, value=value)
}

hiveRm <- function(name) {
  .rhive.rm(name=name)
}
