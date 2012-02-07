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

test.rhive.hdfs <- function()
{
    ## Load emp test data and put it into the Hive
    localData <- system.file(file.path("data", "emp.csv"), package="RHive")
	empTest <- read.csv2(localData, sep=",")
	
	checkTrue(rhive.save(empTest,file="/rhive/unittest/empTest.RData"))
	
	listdata <- rhive.hdfs.ls("/rhive/unittest")
	
	loc <- (listdata['file'] == "/rhive/unittest/empTest.RData")
	checkTrue(length(listdata['file'][loc]) == 1)
	
	rhive.load("/rhive/unittest/empTest.RData")
	
	rhive.hdfs.put(localData,"/rhive/unittest/emp.csv")
	
	listdata <- rhive.hdfs.ls("/rhive/unittest")
	
	loc <- (listdata['file'] == "/rhive/unittest/emp.csv")
	checkTrue(length(listdata['file'][loc]) == 1)

	rhive.hdfs.rename("/rhive/unittest/emp.csv","/rhive/unittest/emp1.csv")
	
	listdata <- rhive.hdfs.ls("/rhive/unittest")
	
	loc <- (listdata['file'] == "/rhive/unittest/emp1.csv")
	checkTrue(length(listdata['file'][loc]) == 1)
	
	rhive.hdfs.chmod("666","/rhive/unittest/emp1.csv")
	listdata <- rhive.hdfs.ls("/rhive/unittest/emp1.csv")
	checkTrue(listdata[['permission']][1] == "rw-rw-rw-")
	
	rhive.hdfs.chown("rhive","/rhive/unittest/emp1.csv")
	listdata <- rhive.hdfs.ls("/rhive/unittest/emp1.csv")
	checkTrue(listdata[['owner']][1] == "rhive")
	
	rhive.hdfs.chgrp("grhive","/rhive/unittest/emp1.csv")
	listdata <- rhive.hdfs.ls("/rhive/unittest/emp1.csv")
	checkTrue(listdata[['group']][1] == "grhive")
	
	rhive.hdfs.rm("/rhive/unittest/emp1.csv")
	
	listdata <- rhive.hdfs.ls("/rhive/unittest")
	
	loc <- (listdata['file'] == "/rhive/unittest/emp1.csv")
	checkTrue(length(listdata['file'][loc]) == 0)
	
	rhive.hdfs.rm("/rhive/unittest/empTest.RData")
	
	queryResult <- rhive.hdfs.du("/rhive")
	checkTrue(!is.null(queryResult))
	
	totalsize <- rhive.hdfs.du("/rhive",summary=TRUE)
	checkTrue(length(totalsize['file']) == 1)
	
}
