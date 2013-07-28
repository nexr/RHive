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



.j2r.Configuration <- function() {
 .jnew("org/apache/hadoop/conf/Configuration")
}

.j2r.EnvUtils <- function() {
  J("com/nexr/rhive/util/EnvUtils")
}

.j2r.HiveJdbcClient <- function(hiveServer2) {
 .jnew("com/nexr/rhive/hive/HiveJdbcClient", hiveServer2)
}

.j2r.JobManager <- function() {
 .jnew("com/nexr/rhive/hadoop/JobManager")
}

.j2r.UDFUtils <- function() {
  J("com/nexr/rhive/hive/udf/UDFUtils")
}

.j2r.FSUtils <- function() {
  J("com/nexr/rhive/hadoop/FSUtils")
}

.j2r.System <- function() {
  J("java/lang/System")
}
