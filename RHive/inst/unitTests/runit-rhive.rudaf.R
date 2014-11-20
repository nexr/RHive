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

test.rhive.rudaf <- function() {

    if(rhive.exist.table("iris")) {
        rhive.drop.table("iris")
    }

    rhive.write.table(iris,"iris")

    sumAllColumns <- function(prev, values) {
        if (is.null(prev)) {
            prev <- rep(0.0, length(values))
        }
        prev + values
    }

    sumAllColumns.partial <- function(values) {
        values
    }

    sumAllColumns.merge <- function(prev, values) {
        if (is.null(prev)) {
            prev <- rep(0.0, length(values))
        }
        prev + values
    }

    sumAllColumns.terminate <- function(values) {
        values
    }

    rhive.assign("sumAllColumns", sumAllColumns)
    rhive.assign("sumAllColumns.partial", sumAllColumns.partial)
    rhive.assign("sumAllColumns.merge", sumAllColumns.merge)
    rhive.assign("sumAllColumns.terminate", sumAllColumns.terminate)

    rhive.exportAll("sumAllColumns")

    queryResult <- rhive.query("SELECT species, RA('sumAllColumns', sepallength, sepalwidth, petallength, petalwidth)
                              FROM iris
                              GROUP BY species")

    checkTrue(!is.null(queryResult))

    if(rhive.exist.table("iris")) {
        rhive.drop.table("iris")
    }
}
