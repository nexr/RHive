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

test.rhive.query <- function()
{
    ## Load emp test data and put it into the Hive
    localData <- system.file(file.path("data", "emp.csv"), package="RHive")
	empTest <- read.csv2(localData, sep=",")
	
	if(rhive.exist.table("empTest")) {
		rhive.query("DROP TABLE empTest")
	}
	
	rhive.write.table(empTest)
	
    queryResult <- rhive.query("select * from empTest")
    checkTrue(!is.null(queryResult))

    if(rhive.exist.table("empTest")) {
		rhive.query("DROP TABLE empTest")
	}
}

test.rhive.array2String <- function()
{
	rhive.drop.table("iris")
	rhive.write.table(iris)
	
	rhive.query("alter table iris replace columns (sepalwidth bigint)")
	
	queryResult <- rhive.query("select array2String(percentile(sepalwidth, array(0,0.2,0.4,0.5,0.99,1))) from iris")
	checkTrue(!is.null(queryResult))

	rhive.drop.table("iris")
}
