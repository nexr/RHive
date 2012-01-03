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

test.rhiveBasic <- function()
{
    ## Load emp test data and put it into the Hive
    localData <- system.file(file.path("data", "emp.csv"), package="RHive")
	empTest <- read.csv2(localData, sep=",")
	
	if(rhive.exist.table("empTest")) {
		rhive.query("DROP TABLE empTest")
	}
	
	rhive.write.table(empTest)

	queryResult <- rhive.basic.mode("empTest","sal")
	checkTrue(!is.null(queryResult))
	
	queryResult <- rhive.basic.range("empTest","sal")
	checkTrue(!is.null(queryResult))

	authors <- data.frame(
         surname = I(c("Tukey", "Venables", "Tierney", "Ripley", "McNeil")),
         nationality = c("US", "Australia", "US", "UK", "Australia"),
         deceased = c("yes", rep("no", 4)))
     books <- data.frame(
         name = I(c("Tukey", "Venables", "Tierney",
                  "Ripley", "Ripley", "McNeil", "R Core")),
         title = c("Exploratory Data Analysis",
                   "Modern Applied Statistics ...",
                   "LISP-STAT",
                   "Spatial Statistics", "Stochastic Simulation",
                   "Interactive Data Analysis",
                   "An Introduction to R"),
         other.author = c(NA, "Ripley", NA, NA, NA, NA,
                          "Venables & Smith"))
                          
    if(rhive.exist.table("authors")) {
    	rhive.query("DROP TABLE authors")
    }
    
    if(rhive.exist.table("books")) {
    	rhive.query("DROP TABLE books")
    }

	rhive.write.table(authors)
	rhive.write.table(books)
	
	queryResult <- rhive.basic.merge("authors","books",by.x="surname",by.y="name")
	checkTrue(!is.null(queryResult))

    DF <- as.data.frame(UCBAdmissions)
    
    if(rhive.exist.table("df")) {
    	rhive.query("DROP TABLE df")
    }

	rhive.write.table(DF)

	queryResult <- rhive.basic.xtabs("freq",c("gender","admit"),"df")
	checkTrue(!is.null(queryResult))

    if(rhive.exist.table("usarrests")) {
    	rhive.query("DROP TABLE usarrests")
    }

	rhive.write.table(USArrests)

    queryResult <- rhive.basic.cut("usarrests","rape",breaks="0:50")
	checkTrue(!is.null(queryResult))

    queryResult <- rhive.basic.cut("usarrests","rape",breaks="0,9,10,30,50")
	checkTrue(!is.null(queryResult))
	
	queryResult <- rhive.basic.cut("usarrests","rape",breaks="0,9,10,30,50", summary=TRUE)
	checkTrue(!is.null(queryResult))

	queryResult <- rhive.basic.by("empTest",c("sal"),c("id","dep"),"sum")
	checkTrue(!is.null(queryResult))

    if(rhive.exist.table("empTest")) {
		rhive.query("DROP TABLE empTest")
	}
}