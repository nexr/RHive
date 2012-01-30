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

test.rhive.exportAll <- function()
{
    ## Load emp test data and put it into the Hive
    localData <- system.file(file.path("data", "emp.csv"), package="RHive")
	empTest <- read.csv2(localData, sep=",")
	
	if(rhive.exist.table("empTest")) {
		rhive.query("DROP TABLE empTest")
	}
	
	rhive.write.table(empTest)
	
    userconf <- 5
    usercal2 <- function(sal) {
		sal * userconf
    }
	
    checkTrue(rhive.assign('usercal2',usercal2))
    checkTrue(rhive.assign('userconf',userconf))
    checkTrue(rhive.exportAll('usercal2'))
	
    queryResult <- rhive.query("select R('usercal2',sal,0.0) from empTest")
    checkTrue(!is.null(queryResult))

    if(rhive.exist.table("empTest")) {
		rhive.query("DROP TABLE empTest")
	}
}
