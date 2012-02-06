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

test.rhive.napply <- function() {

	rhive.drop.table('irisTest')
	rhive.write.table(iris,'irisTest')
	
	queryResult <- rhive.napply('irisTest',function(col) { col * 2 }, 'petallength')
	checkTrue(!is.null(queryResult))

}

test.rhive.sapply <- function() {

	rhive.drop.table('irisTest')
	rhive.write.table(iris,'irisTest')
	
	queryResult <- rhive.sapply('irisTest',function(col) { paste('rhive-',col,sep='') }, 'species')
	checkTrue(!is.null(queryResult))
}


test.rhive.aggregate <- function() {

	rhive.drop.table('irisTest')
	rhive.write.table(iris,'irisTest')
	
	queryResult <- rhive.aggregate('irisTest','avg','petallength',groups=c("species"))
	checkTrue(!is.null(queryResult))

}