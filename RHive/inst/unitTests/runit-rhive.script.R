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

 
stopifnot(require(RHive, quietly=TRUE))
stopifnot(require(RUnit, quietly=TRUE))


test.rhive.mapapply <- function() {

    ## Load emp test data and put it into the Hive
    localData <- system.file(file.path("data", "emp.csv"), package="RHive")
	empTest <- read.csv2(localData, sep=",")
	
	if(rhive.exist.table("empTest")) {
		rhive.query("DROP TABLE empTest")
	}
	
	rhive.write.table(empTest)
	
	map <- function(k,v) {
	
	  if(is.null(v)) {
         put(NA,1)
      }
      lapply(v, function(vv) {
         lapply(strsplit(x=vv,split ="\t")[[1]], function(w) put(paste(args,w,sep=""),1))
      })
	
	}

	queryResult <- rhive.mapapply("empTest",map,c("ename","position"),c("position","one"),by="position",forcedRef=FALSE)
	checkTrue(!is.null(queryResult))

    if(rhive.exist.table("empTest")) {
		rhive.query("DROP TABLE empTest")
	}


}


test.rhive.mrapply <- function()
{
    ## Load emp test data and put it into the Hive
    localData <- system.file(file.path("data", "emp.csv"), package="RHive")
	empTest <- read.csv2(localData, sep=",")
	
	if(rhive.exist.table("empTest")) {
		rhive.query("DROP TABLE empTest")
	}
	
	rhive.write.table(empTest)
	
	map <- function(k,v) {
	
	  if(is.null(v)) {
         put(NA,1)
      }
      lapply(v, function(vv) {
         lapply(strsplit(x=vv,split ="\t")[[1]], function(w) put(paste(args,w,sep=""),1))
      })
	
	}
	
	reduce <- function(k,vv) {
  
      put(k,sum(as.numeric(vv)))
  
	}
	
	queryResult <- rhive.mrapply("empTest",map,reduce,c("ename","position"),c("position","one"),by="position",c("position","one"),c("word","count"),forcedRef=FALSE)
	
    checkTrue(!is.null(queryResult))
    checkTrue(length(row.names(queryResult)) == 5)

    if(rhive.exist.table("empTest")) {
		rhive.query("DROP TABLE empTest")
	}
}
