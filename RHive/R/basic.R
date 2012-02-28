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


rhive.basic.mode <- function(tablename, col, forcedRef = TRUE) {

	if(missing(tablename))
		stop("missing tablename")
	if(missing(col))
		stop("missing colname")
			
	tablename <- tolower(tablename)
	col <- tolower(col)

	hql <- sprintf("SELECT %s , COUNT(1) freq FROM %s GROUP BY %s ORDER BY freq DESC LIMIT 1", col, tablename, col)

	if(forcedRef)
		result <- rhive.big.query(hql,memlimit=-1)
	else
		result <- rhive.big.query(hql)

	return(result)

}

rhive.basic.range <- function(tablename, col) {

	if(missing(tablename))
		stop("missing tablename")
	if(missing(col))
		stop("missing colname")
			
	tablename <- tolower(tablename)
	col <- tolower(col)


    hql <- sprintf("SELECT MIN(%s) min, MAX(%s) max FROM %s", col, col, tablename)
	
	result <- rhive.query(hql)
	return(c(result[['min']],result[['max']]))
}

rhive.basic.merge <- function(x, y, by.x, by.y, forcedRef = TRUE) {

	if(missing(x))
		stop("missing parameter(first tablename)")
	if(missing(y))
		stop("missing parameter(second tablename)")
			
	x <- tolower(x)
	y <- tolower(y)	

	xcols <- rhive.desc.table(x)[,'col_name']
	ycols <- rhive.desc.table(y)[,'col_name']
	
	joinkeys <- NULL
	yjoinkeys <- NULL

	if(missing(by.x) && missing(by.y)) {
	
		joinkeys <- intersect(xcols, ycols)
		yjoinkeys <- joinkeys
		
  	}else if(missing(by.y)) {
  		
  		joinkeys <- by.x
  		yjoinkeys <- joinkeys
  		
  	}else if(missing(by.x)) {
  		
  		joinkeys <- by.y
  		yjoinkeys <- joinkeys
  	
  	}else {
  	
  		joinkeys <- by.x
  		yjoinkeys <- by.y
  	
  	}
  	
  	unioncols <- c(setdiff(xcols, joinkeys), setdiff(ycols, yjoinkeys))
  	
  	sharedcols <- intersect(setdiff(xcols, joinkeys),setdiff(ycols, yjoinkeys))
  	
  	if(length(sharedcols) > 0) {
  		unioncols <- c(setdiff(unioncols, sharedcols), paste("x.",sharedcols,sep=""))
  	}else {
  		unioncols <- c(paste("x.",setdiff(xcols,joinkeys),sep=""),paste("y.",setdiff(ycols,yjoinkeys),sep=""))
  	}
  		
	hql <- paste("SELECT ", paste("x.", joinkeys, sep="", collapse=","), ",", paste(unioncols,collapse=","), " FROM ", x, " x JOIN ", y, " y ", sep="")
	
	if(!is.null(joinkeys)) {
		where <- ""
		for (i in 1:length(joinkeys)) {  
	 
	  		if (i==1) {
	  			where <- paste("ON x.", joinkeys[i], " = y.", yjoinkeys[i], sep="")  
	  		} else { 
	  			where <- paste(where, paste(" and x.", joinkeys[i], " = y.", yjoinkeys[i], sep=""))
	  	    }
	  	}
	  	
	  	hql <- paste(hql, where)
  	}

	if(forcedRef)
		result <- rhive.big.query(hql,memlimit=-1)
	else
		result <- rhive.big.query(hql)

	return(result)
}

rhive.basic.xtabs <- function(formula, tablename) {

	if(missing(formula))
		stop("missing formula")
	if(missing(tablename))
		stop("missing tablename")

	cformula <- as.character(formula)
	
	if(length(cformula)==3)
		x <- cformula[2]
	
	cols <- ifelse(length(cformula)==3,cformula[3],cformula[2])
	cols <- unlist(strsplit(gsub(" ", "", cols),"\\+"))
			
	tablename <- tolower(tablename)
	cols <- tolower(cols)

	gcols <- .generateColumnString(cols)

	if(length(cformula)==3) {
		hql <- sprintf("SELECT %s, SUM(%s) %s FROM %s GROUP BY %s", gcols, x, x, tablename, gcols)
	}else {
		hql <- sprintf("SELECT %s, COUNT(1) x_count FROM %s GROUP BY %s", gcols, tablename, gcols)
	}
	
	pivotresult <- rhive.query(hql)
	
	fcols <- .generateColumnString(cols,sep="+")
	formula <- ""
	
	if(length(cformula)==3) {
		formula <- sprintf("%s ~ %s",x,fcols)
	}else {
		formula <- sprintf("%s ~ %s","x_count",fcols)
	}
	
	return(xtabs(formula, pivotresult)) 

}

rhive.basic.cut <- function(tablename, col, breaks, right=TRUE, summary = FALSE, forcedRef = TRUE) {
	
	if(missing(tablename))
		stop("missing tablename")
	if(missing(breaks))
		stop("missing breaks")
		
	if(missing(col) || is.null(col)) {
		x <- unlist(strsplit(tablename,"\\$"))
		
		if(length(x) != 2)
			stop("missing colname")
		
		tablename <- x[1]
		col <- x[2]	
	}
		
	tablename <- tolower(tablename)
	col <- tolower(col)	
		
	if(is.vector(breaks) && !is.character(breaks)) {
	
		if(length(breaks) == 1L) {
			if(is.na(breaks) | breaks < 2L)
				stop("invalid number of intervals")
				
			nb <- as.integer(breaks + 1)
			range <- rhive.basic.range(tablename, col)
			dx <- diff(range)
			if(dx == 0)
				dx <- abs(range[1L])
			breaks <- seq.int(range[1L] - dx / (breaks * 1000), range[2L] + dx / (breaks * 1000), length.out = nb)
		}
		
		breaks <- paste(breaks,collapse=",")
	}
	
	if(summary) {
		hql <- sprintf("SELECT rkey(%s,'%s','%s'), COUNT(%s) FROM %s GROUP BY rkey(%s,'%s','%s')",col,breaks,right, col,tablename,col,breaks,right)
	
		tmp <- rhive.query(hql)
		
		result <- unlist(tmp['X_c1'])
		names(result) <- unlist(tmp['X_c0'])
		
		return(result)
	}else {
	
		hql <- ""
	    xcols <- rhive.desc.table(tablename)[,'col_name']
		cols <- setdiff(xcols, col)
		
		if(length(cols) > 0) {
			hql <- sprintf("SELECT %s, rkey(%s,'%s','%s') %s FROM %s",paste(cols, collapse=","),col,breaks,right,col,tablename)
		}else {
			hql <- sprintf("SELECT rkey(%s,'%s','%s') %s FROM %s", col,breaks,right,col,tablename)
		}
		
		if(forcedRef)
			result <- rhive.big.query(hql,memlimit=-1)
		else
			result <- rhive.big.query(hql)
	
		return(result)
	}
	
}

rhive.basic.cut2 <- function(tablename, col1, col2, breaks1, breaks2, right=TRUE, keepCol = FALSE, forcedRef = TRUE) {
	
	if(missing(tablename))
		stop("missing tablename")
	if(missing(breaks1))
		stop("missing breaks1")
	if(missing(breaks2))
		stop("missing breaks2")
		
	if(missing(col1) || is.null(col1)) {
		stop("missing colname")	
	}
	if(missing(col2) || is.null(col2)) {
		stop("missing colname")	
	}
		
	tablename <- tolower(tablename)
	col1 <- tolower(col1)	
	col2 <- tolower(col2)
		
	if(is.vector(breaks1) && !is.character(breaks1)) {
	
		if(length(breaks1) == 1L) {
			if(is.na(breaks1) | breaks1 < 2L)
				stop("invalid number of intervals")
				
			nb <- as.integer(breaks1 + 1)
			range <- rhive.basic.range(tablename, col1)
			dx <- diff(range)
			if(dx == 0)
				dx <- abs(range[1L])
			breaks1 <- seq.int(range[1L] - dx / (breaks1 * 1000), range[2L] + dx / (breaks1 * 1000), length.out = nb)
		}
		
		breaks1 <- paste(breaks1,collapse=",")
	}
	
	if(is.vector(breaks2) && !is.character(breaks2)) {
	
		if(length(breaks2) == 1L) {
			if(is.na(breaks2) | breaks2 < 2L)
				stop("invalid number of intervals")
				
			nb <- as.integer(breaks2 + 1)
			range <- rhive.basic.range(tablename, col2)
			dx <- diff(range)
			if(dx == 0)
				dx <- abs(range[1L])
			breaks2 <- seq.int(range[1L] - dx / (breaks2 * 1000), range[2L] + dx / (breaks2 * 1000), length.out = nb)
		}
		
		breaks2 <- paste(breaks2,collapse=",")
	}
	
	
	hql <- ""
    xcols <- rhive.desc.table(tablename)[,'col_name']
	cols <- setdiff(xcols, c(col1,col2))
	
	if(keepCol) {
		if(length(cols) > 0) {
			hql <- sprintf("SELECT %s, %s, rkey(%s,'%s','%s') %s, %s, rkey(%s,'%s','%s') %s , 1 as (rep) FROM %s",paste(cols, collapse=","),col1,col1,breaks1,right,paste(col1,"_cut",sep=""),col2, col2,breaks2,right,paste(col2,"_cut",sep=""), tablename)
		}else {
			hql <- sprintf("SELECT %s, rkey(%s,'%s','%s') %s , %s, rkey(%s,'%s','%s') %s 1 as (rep) FROM %s", col1, col1,breaks1,right,paste(col1,"_cut",sep=""),col2, col2,breaks2,right,paste(col2,"_cut",sep=""),tablename)
		}	
	}else {
		if(length(cols) > 0) {
			hql <- sprintf("SELECT %s, rkey(%s,'%s','%s') %s, rkey(%s,'%s','%s') %s , 1 as (rep) FROM %s",paste(cols, collapse=","),col1,breaks1,right,col1,col2,breaks2,right,col2, tablename)
		}else {
			hql <- sprintf("SELECT rkey(%s,'%s','%s') %s , rkey(%s,'%s','%s') %s 1 as (rep) FROM %s", col1,breaks1,right,col1,col2,breaks2,right,col2,tablename)
		}
	}
	
	if(forcedRef)
		result <- rhive.big.query(hql,memlimit=-1)
	else
		result <- rhive.big.query(hql)

	return(result)
	
}

rhive.basic.by <- function(tablename, INDICES, fun, arguments, forcedRef = TRUE) {

	if(missing(arguments))
		stop("missing arguments")
	if(missing(tablename))
		stop("missing tablename")
	if(missing(INDICES))
		stop("missing INDICES")
	if(missing(fun))
		stop("missing fun")
			
	tablename <- tolower(tablename)

	arguments <- paste(arguments,collapse=",")
    colnames <- paste(fun, "(", arguments, ") ",fun, sep="", collapse=",")
	groups <- paste(INDICES, collapse=",")

	hql <- sprintf("SELECT %s, %s FROM %s GROUP BY %s",groups,colnames,tablename,groups)
	
	if(forcedRef)
		result <- rhive.big.query(hql,memlimit=-1)
	else
		result <- rhive.big.query(hql)

	return(result)
}

rhive.basic.scale <- function(tablename, col) {

	if(missing(tablename))
		stop("tablename name is not set.")
			
	tablename <- tolower(tablename)
	col <- tolower(col)

	hql <- sprintf("SELECT AVG(%s) avg, STD(%s) std FROM %s",col,col,tablename)
	summary <- rhive.query(hql)
	
	avg <- summary[['avg']]
	std <- summary[['std']]
	
	tmpTable <- paste("cut_", tablename,as.integer(Sys.time()),sep="")
	xcols <- rhive.desc.table(tablename)[,'col_name']
	cols <- setdiff(xcols, col)

	hql <- sprintf("CREATE TABLE %s AS SELECT %s, %s, scale(%s,%s,%s) %s FROM %s",tmpTable,paste(cols, collapse=","),col, col,avg,std,paste("sacled_",col,sep=""),tablename)
	rhive.query(hql)
	
	x <- tmpTable
	attr(x,"scaled:center") <- avg
	attr(x,"scaled:scale") <- std
	
	x
}



rhive.basic.t.test <- function(x,col1,y,col2) {

	if(missing(x) || missing(y))
		stop("tablename name is not set.")
	if(missing(col1) || missing(col2))
		stop("column name is not set.")

	tableX <- x
	colX <- col1	

	tableY <- y
	colY <- col2	

	resultX <- rhive.query(paste("select variance(",colX,"), avg(",colX,"), count(",colX,") from ",tableX,sep=""))
    resultY <- rhive.query(paste("select variance(",colY,"), avg(",colY,"), count(",colY,") from ",tableY,sep=""))

	varX <- resultX[[1]]
	varY <- resultY[[1]]
	meanX <- resultX[[2]]
	meanY <- resultY[[2]]
	countX <- resultX[[3]]
	countY <- resultY[[3]]
	
	t <- (meanX - meanY) / sqrt(varX/countX + varY/countY)
	df <- (varX / countX + varY / countY)^2 / ((varX^2 / (countX^2 * (countX-1))) + (varY^2 / (countY^2 * (countY - 1))))
	p <- (1-pt(abs(t), df=df)) * 2


	message <- sprintf("t = %s, df = %s, p-value = %s, mean of x : %s, mean of y : %s", t, df, p, meanX, meanY)
	print(message)
	
	result <- list()
	
	result[[1]] <- t
	result[[2]] <- df
	result[[3]] <- p
	result[[4]] <- list()
	result[[4]][[1]] <- meanX
	result[[4]][[2]] <- meanY
	names(result[[1]]) <- "t"
	names(result[[2]]) <- "df"
	names(result[[4]][[1]]) <- "mean of x"
	names(result[[4]][[2]]) <- "mean of y" 
	
	names(result) <- c("statistic","parameter","p.value","estimate")
	
	return(result)
}

rhive.block.sample <- function(tablename, percent = 0.01, seed = 0, subset) {
			
	if(missing(tablename))
		stop("tablename name is not set.")		
			
	tablename <- tolower(tablename)

    rhive.query(paste("set hive.sample.seednumber=",seed,sep=""))
     
    tmptable <- paste("rhive_sblk_",as.integer(Sys.time()),sep="")
    if(missing(subset) || is.null(subset)) {
    	hql <- paste("CREATE TABLE",tmptable,"AS SELECT * FROM",tablename,"TABLESAMPLE(",percent,"PERCENT)") 
    	rhive.query(hql)
    }else {
        stmptable <- paste("rhive_subset_",as.integer(Sys.time()),sep="")
    	hql <- paste("CREATE TABLE",stmptable,"AS SELECT * FROM",tablename,"WHERE",subset) 
        rhive.query(hql)
    
    	hql <- paste("CREATE TABLE",tmptable,"AS SELECT * FROM",stmptable,"TABLESAMPLE(",percent,"PERCENT)") 
    	rhive.query(hql)
	    rhive.drop.table(stmptable)
	}

	return(tmptable)
}


.generateColumnString <- function(columns,sep=",",excludes) {

    colindex <- 0
    cols <- ""

    for(element in columns) {
         cols <- paste(cols,element,sep="")    
         if(colindex < length(columns) - 1) {
         	cols <- paste(cols, sep ,sep="")
         }
         colindex <- colindex + 1
    }

    return(cols)
}