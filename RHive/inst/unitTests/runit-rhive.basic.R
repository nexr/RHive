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

test.rhive.basic.merge <- function() {

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
                
                
    rhive.drop.table("authors")          
	rhive.drop.table("books") 
	rhive.drop.table("iris")
	rhive.drop.table("jiris")

	rhive.write.table(authors)
	rhive.write.table(books)
	rhive.write.table(iris)
	rhive.write.table(iris,"jiris")
	
	queryResult <- rhive.basic.merge("authors","books",by.x="surname",by.y="name")
	checkTrue(!is.null(queryResult))
	
	queryResult <- rhive.basic.merge("iris","jiris",by.x="sepallength",by.y="sepallength")
	checkTrue(!is.null(queryResult))
	
    rhive.drop.table("authors")          
	rhive.drop.table("books") 
	rhive.drop.table("iris")
	rhive.drop.table("jiris")

    try(rm(books), silent=TRUE)
	try(rm(authors), silent=TRUE)

}

test.rhive.basic.cut <- function() {

	rhive.drop.table("usarrests")

	rhive.write.table(USArrests)

    queryResult <- rhive.basic.cut("usarrests","rape",breaks="0:50")
	checkTrue(!is.null(queryResult))

    queryResult <- rhive.basic.cut("usarrests","rape",breaks="0,9,10,30,50")
	checkTrue(!is.null(queryResult))
	
	queryResult <- rhive.basic.cut("usarrests","rape",breaks="30,35,50")
	checkTrue(!is.null(queryResult))
	
	queryResult <- rhive.basic.cut("usarrests","rape",breaks="0,9,10,30,50", summary=TRUE)
	checkTrue(!is.null(queryResult))

	queryResult <- rhive.basic.cut("usarrests","rape", right=FALSE, breaks=0:50)
	checkTrue(!is.null(queryResult))

	rhive.drop.table("usarrests")

}

test.rhive.basic.cut2 <- function() {

	rhive.drop.table("usarrests")

	rhive.write.table(USArrests)

    queryResult <- rhive.basic.cut2("usarrests","rape","urbanpop",breaks1="0:50",breaks2="0:100", forcedRef=FALSE)
	checkTrue(!is.null(queryResult))
	
	queryResult <- rhive.basic.cut2("usarrests","rape","urbanpop",breaks1="0:50",breaks2="0:100", keepCol=TRUE, forcedRef=FALSE)
	checkTrue(!is.null(queryResult))

	rhive.drop.table("usarrests")

}


test.rhive.basic.xtabs <- function() {

	DF <- as.data.frame(UCBAdmissions)
    
    if(rhive.exist.table("df")) {
    	rhive.query("DROP TABLE df")
    }

	rhive.write.table(DF)

	queryResult <- rhive.basic.xtabs(freq ~gender + admit,"df")
	checkTrue(!is.null(queryResult))

    if(rhive.exist.table("df")) {
    	rhive.query("DROP TABLE df")
    }
    
    try(rm(DF), silent=TRUE)
}

test.rhive.basic.mode <- function() {

    ## Load emp test data and put it into the Hive
    localData <- system.file(file.path("data", "emp.csv"), package="RHive")
	empTest <- read.csv2(localData, sep=",")
	
	if(rhive.exist.table("empTest")) {
		rhive.query("DROP TABLE empTest")
	}
	
	rhive.write.table(empTest)


	queryResult <- rhive.basic.mode("empTest","sal")
	checkTrue(!is.null(queryResult))
	
    if(rhive.exist.table("empTest")) {
		rhive.query("DROP TABLE empTest")
	}
	
	try(rm(empTest), silent=TRUE)
	try(rm(localData), silent=TRUE)
}

test.rhive.basic.range <- function() {

    ## Load emp test data and put it into the Hive
    localData <- system.file(file.path("data", "emp.csv"), package="RHive")
	empTest <- read.csv2(localData, sep=",")
	
	if(rhive.exist.table("empTest")) {
		rhive.query("DROP TABLE empTest")
	}
	
	rhive.write.table(empTest)

	queryResult <- rhive.basic.range("empTest","sal")
	checkTrue(!is.null(queryResult))
	
    if(rhive.exist.table("empTest")) {
		rhive.query("DROP TABLE empTest")
	}
	
	try(rm(empTest), silent=TRUE)
	try(rm(localData), silent=TRUE)
}

test.rhive.basic.by <- function() {

    ## Load emp test data and put it into the Hive
    localData <- system.file(file.path("data", "emp.csv"), package="RHive")
	empTest <- read.csv2(localData, sep=",")
	
	if(rhive.exist.table("empTest")) {
		rhive.query("DROP TABLE empTest")
	}
	
	rhive.write.table(empTest)

	queryResult <- rhive.basic.by("empTest",c("id","dep"),"sum",c("sal"))
	checkTrue(!is.null(queryResult))
	
    if(rhive.exist.table("empTest")) {
		rhive.query("DROP TABLE empTest")
	}
	
	try(rm(empTest), silent=TRUE)
	try(rm(localData), silent=TRUE)

}

test.rhive.basic.scale <- function() {

    ## Load emp test data and put it into the Hive
    localData <- system.file(file.path("data", "emp.csv"), package="RHive")
	empTest <- read.csv2(localData, sep=",")
	
	if(rhive.exist.table("empTest")) {
		rhive.query("DROP TABLE empTest")
	}
	
	rhive.write.table(empTest)

	queryResult <- rhive.basic.scale("empTest","sal")
	checkTrue(!is.null(queryResult))
	
    if(rhive.exist.table("empTest")) {
		rhive.query("DROP TABLE empTest")
	}
	
	try(rm(empTest), silent=TRUE)
	try(rm(localData), silent=TRUE)

}

test.rhive.basic.t.test <- function() {


	x <- 1:10
	y <- 7:20
	testX <- data.frame(x)
	testY <- data.frame(y)
	
	rhive.drop.table("testX")
	rhive.drop.table("testY")
	
	rhive.write.table(testX)
	rhive.write.table(testY)
	
	queryResult <- rhive.basic.t.test("testX","x","testY","y")
	checkTrue(!is.null(queryResult$p.value))
	
	rhive.drop.table("testX")
	rhive.drop.table("testY")
}


test.rhive.block.sample <- function() {

	## Load emp test data and put it into the Hive
    localData <- system.file(file.path("data", "emp.csv"), package="RHive")
	empTest <- read.csv2(localData, sep=",")
	rhive.drop.table("empTest")
	
	rhive.write.table(empTest)
	
	queryResult <- rhive.block.sample("emptest")
	checkTrue(rhive.exist.table(queryResult))

	rhive.drop.table(queryResult)
}