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


library(utils)
.rhiveEnv <- new.env()
.rhiveExportEnv <- new.env()

.onLoad <- function(libname, pkgname) {
#   .jpackage(pkgname, lib.loc = libname)
	pkdesc <- packageDescription(pkgname, lib.loc = libname, fields = "Version", drop = TRUE)
        
#   if (Sys.getenv("HIVE_HOME") == "") stop(sprintf("Environment variable HIVE_HOME must be set before loading package %s", pkgname))
    
    packageStartupMessage("This is ", pkgname, " ", pkdesc, ". ",
                        "For overview type ", sQuote(paste("?", pkgname, sep="")), ".",  
                        "\nHIVE_HOME=", Sys.getenv("HIVE_HOME"))

    if (Sys.getenv("HIVE_HOME") != "") {
    	rhive.init()
    	packageStartupMessage("call rhive.init() because HIVE_HOME is set.")
    }else {
    	packageStartupMessage("must call rhive.init before using rhive.")
    }
}