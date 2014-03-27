# Copyright (c) 2014 Oliver Keyes
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#Function for reading the data in.
data_reader <- function(){
  
  #I really, really need to build that hadoop/RHive vagrant instance and convince Ops we need that.
  #Retrieve the IPs, save them to a table.
  system("hive --auxpath /usr/lib/hcatalog/share/hcatalog/hcatalog-core-0.5.0-cdh4.3.1.jar --database wmf -e \"
         set hive.mapred.mode = nonstrict;
         INSERT OVERWRITE TABLE ironholds.distinct_ip
         SELECT distip FROM (
         SELECT ip AS distip, COUNT(*) as count FROM wmf.webrequest_mobile WHERE year = 2014 AND month = 1 AND day BETWEEN 23 AND 30 AND user_agent NOT IN (\'ativeHost\',\'NativeHost\') AND cache_status = \'hit\' AND http_status IN (\'200\',\'301\',\'302\',\'304\') AND user_agent NOT IN (\'NativeHost\',\'ativeHost\') AND content_type IN (\'text/html\\; charset=utf-8\',\'text/html\\; charset=iso-8859-1\',\'text/html\\; charset=UTF-8','text/html\') GROUP BY ip ORDER BY rand()
         ) sub1 WHERE count >= 2 LIMIT 50000;\""
  )
  
  #retrieve the dataset as a whole, save it to file.
  system(paste("hive --auxpath /usr/lib/hcatalog/share/hcatalog/hcatalog-core-0.5.0-cdh4.3.1.jar --database wmf -e \"
         SELECT
          db1.dt,
          db1.ip,
          db1.uri_host,
          db1.uri_path,
          db1.uri_query,
          db1.referer,
          db1.user_agent,
          db1.accept_language
        FROM wmf.webrequest_mobile db1 INNER JOIN ironholds.distinct_ip db2 ON db1.ip = db2.ip
        WHERE db1.year = 2014 AND db1.month = 1 AND db1.day BETWEEN 23 AND 30 AND db1.user_agent NOT IN (\'ativeHost\',\'NativeHost\') AND user_agent NOT IN (\'NativeHost\',\'ativeHost\')
        AND db1.http_status IN (200,301,302,304) AND db1.content_type IN (\'text/html\\; charset=utf-8\',\'text/html\\; charset=iso-8859-1\',\'text/html\\; charset=UTF-8','text/html\');\" >",mobile_file))
  
  #Read in the file.
  data.df <- read.delim(file = mobile_file,
                        as.is = TRUE,
                        header = TRUE,
                        quote = "",
                        col.names = c("timestamp",
                                      "IP",
                                      "URL_host",
                                      "URL_page",
                                      "URL_query",
                                      "referer",
                                      "UA",
                                      "lang"))
  
  #Return
  return(data.df)
}

#The MySQL querying function
rawsql <- function(statement,db_host,db_database){
  
  #Open connection to the MySQL DB
  con <- dbConnect(drv = "MySQL",
                   username = db_user,
                   password = db_pass,
                   host = db_host,
                   dbname = db_database)
  
  QuerySend <- dbSendQuery(con, statement)
  
  #Retrieve output of query
  output <- fetch(QuerySend, n = -1)
  
  #Kill connection
  dbDisconnect(con)
  
  #Return output
  return(output)
}

#Function for querying all production dbs
globalsql = function(IPs_query){
  
  #Connect to each server...
  query_results.ls <- lapply(server_list, function(x){
    
    #Find out what the available dbs are
    available.dbs <- rawsql(statement = "SHOW DATABASES;", db_host = x, db_database = NULL)
    
    #Filter the available dbs to active wikis
    available.dbs <- available.dbs$Database[available.dbs$Database %in% active_wikis]
    
    #For each one, grab the data the statement requires.
    #Output object. It has to be a data frame unless we want recursive do.call("rbind"), which I'm considering
    output.ls <- list()
    
    for(i in seq_along(available.dbs)){
      
      #Retrieve the data and bind it into the output object
      output.ls[[(length(output.ls)+1)]] <- rawsql(statement = IPs_query, db_host = x, db_database = available.dbs[i])
      
    }
    
    #Turn the SQL results into a single data frame
    output.df <- do.call("rbind",output.ls)
    
    #Return
    return(output.df)
  })
  
  #Turn the SQL results into a single data frame and return them
  return(do.call("rbind",query_results.ls))
  
}

#Hashing and filtering function
filter <- function(x, ts_format = "%Y-%m-%dT%H:%M:%S", prehashed = FALSE){
  
  #Filter out rows with no timestamp
  x <- x[!x$timestamp == "",]
  
  if(prehashed == FALSE){
    
    #Eliminate bots
    x <- x[!grepl(x = x$UA, ignore.case = TRUE, pattern = bot_pattern),]
    
    #Generate SHA-256 unique hashes
    hash_vec <- character(nrow(x))
    
    for(i in seq_along(hash_vec)){
      
      hash_vec[i] <- digest(object = paste(x$IP[i],x$lang[i],x$UA[i]), algo = "sha256")
      
    }
    
    #Add the hash vector to the dataset
    x$hash <- hash_vec
  }
  
  #Limit to those hashes with >1 article view
  aggs <- as.data.frame(table(x$hash))
  x <- x[x$hash %in% subset(aggs, Freq > 1)$Var1,]
  print(nrow(aggs[aggs$Freq == 1,]))
  
  #Convert timestamps to seconds
  x$timestamp <- as.numeric(strptime(x = x$timestamp, format = ts_format))
  
  #Strip unnecessary columns
  x <- x[,c("timestamp","hash")]
  
  #Return
  return(x)
  
}

#Intertime function
lapply_inter <- function(x,dataset){
  
  #Run through C++
  intervals <- intertime(x = dataset$timestamp[dataset$hash == x])
  
  #Return
  return(intervals)
}
  
#From-first function
lapply_first <- function(x,dataset){
  
  #Run through C++
  first_intervals <- fromfirst(x = dataset$timestamp[dataset$hash == x])
  
  #Return
  return(first_intervals)
  
}
  
#lapply function
lapper <- function(dataset, func, filename){
  
  #Grab list
  results.ls <- lapply(X = as.list(unique(dataset$hash)), FUN = func, dataset = dataset)
  
  #Save to RData file for future screwin'-with.
  save(results.ls, file = filename)
  
  #Unlist and summarise
  aggregates.df <- as.data.frame(table(unlist(results.ls)))
  
  #Change type
  aggregates.df$Var1 <- as.numeric(as.character(aggregates.df$Var1))

  #Return
  return(list(results.ls,aggregates.df))

}

#Initial graphing
grapher <- function(x, folder, datatype){
  
  #Generate a log10 plot and save
  log10_plot <- ggplot(data = x, aes(log10(Freq))) +
    geom_area(stat = "bin", fill = "blue") +
    labs(title = paste("Log10 plot of time intervals\n(",datatype,")"),
         x = "Log10",
         y = "Number of occurrences")
  ggsave(file = file.path(getwd(),folder,paste(datatype,"log10.png",sep = "_")),
         plot = log10_plot)
  
  #Smoothed plot
  smooth_plot <- ggplot(data = x, aes(Var1,Freq)) + 
    geom_smooth() + 
    labs(title = paste("Inter-time periods for mobile web requests, unlimited range\n(",datatype,")"),
         x = "Seconds",
         y = "Number of requests")
  ggsave(file = file.path(getwd(),folder,paste(datatype,"smooth.png",sep = "_")),
         plot = smooth_plot)

  #Smoothed, limited plot
  limited_plot <- ggplot(data = x, aes(Var1,Freq)) + 
    geom_smooth() + 
    labs(title = paste("Inter-time periods for mobile web requests, limited range\n(",datatype,")"),
         x = "Seconds",
         y = "Number of requests") +
    scale_x_continuous(breaks = seq(0,3000,100), limits = c(0,3000))
ggsave(file = file.path(getwd(),folder,paste(datatype,"limited_smooth.png",sep = "_")),
       plot = limited_plot)
  
}

#Post-minimum-identification analysis
sessionlength <- function(x, y, local_minimum, datatype){
  
  #Instantiate output object
  output.vec <- numeric(length = length(x))
  
  for(i in seq_along(output.vec)){
    
    #Retrieve 'session time' and add it to the output vector.
    output.vec[i] <- totaltime(x = x[[i]], local_minimum = local_minimum)
    
  }
  
  #Remove zero entries (entries where all datapoints were above the local minimum)
  output.vec <- output.vec[!output.vec == 0]
  
  #Dataframe it for ggplot2
  output.df <- as.data.frame(output.vec)
  
  #What does the total density look like?
  totaltime_density <- ggplot(data = output.df, aes(x = output.vec)) + 
    geom_density(fill = "blue") +
    labs(title = paste("Total mobile session times\n",datatype),
         x = "Seconds",
         y = "Density")
  ggsave(file = file.path(getwd(),"Data",paste(datatype,"density.png",sep = "_")),
         plot = totaltime_density)
  
  #<75th quantile?
  quantiles <- quantile(output.vec)
  output_restricted.df <- as.data.frame(output.df[output.df$output.vec <= quantiles[names(quantiles) == "75%"],])
  names(output_restricted.df) <- "output"
  totaltime_density_75 <- ggplot(data = output_restricted.df, aes(x = output)) + 
    geom_density(fill = "blue") +
    labs(title = paste("Total mobile session times\n <75th percentile,",datatype),
         x = "Seconds",
         y = "Density")
  ggsave(file = file.path(getwd(),"Data",paste(datatype,"density_75.png",sep = "_")),
         plot = totaltime_density_75)
  
  #Log10 bins?
  totaltime_log10 <- ggplot(data = output.df, aes(x = output.vec)) + 
    geom_density(fill = "blue") +
    labs(title = paste("Total mobile session times\nlog10,",datatype),
         x = "Seconds (log10)",
         y = "Density") +
    scale_x_log10()
  ggsave(file = file.path(getwd(),"Data",paste(datatype,"density_log10.png",sep = "_")),
         plot = totaltime_log10)
  
  #Save quantiles. Well, the substantial ones.
  write.table(x = quantile(output.vec,seq(0,1,0.10)),
              file = file.path(getwd(),"Data",paste(datatype,"quantiles.tsv",sep = "_")),
              sep = "\t")
  
  #Work out the time spent on each page
  in_session_intertime <- unlist(lapply(y, function(x,local_minimum){
    
    #Output object
    output <- numeric()
    
    #Check for errors
    if(sum(is.na(x)) == 0){
      
      #For each element
      for(i in seq_along(x)){
        
        #If the element < the local minimum..
        if(x[i] <= local_minimum){
          
          #Append it to the output
          output[i] <- x[i]
          
        } else {
          
          #Return when you encounter the first element above the local minimum
          return(output)
          
        }
      }
      
      #If you don't encounter any, return at the end
      return(output)
    }
  }, local_minimum = local_minimum))
  
  #Save
  save(in_session_intertime, file = file.path(getwd(),"Data",paste(datatype,"in_session_intertime.RData")))
  
  #Format and Visualise
  box.df <- as.data.frame(in_session_intertime)
  box.df$type <- "Data"
  error_bar_plot <- ggplot(data = box.df,
                           aes(type,in_session_intertime)) +
    geom_boxplot(aes(fill = type)) +
    theme(axis.ticks = element_blank(), axis.text.x = element_blank()) +
    labs(title = paste("Time spent on each page, Mobile readers\n",datatype,sep = ""),
         x = "",
         y = "Seconds") +
    guides(fill=FALSE) +
    scale_y_continuous(breaks = seq(0,max(box.df$in_session_intertime),25))
  ggsave(file = file.path(getwd(),"Data",paste(datatype,"boxplot.png",sep = "_")),
         plot = error_bar_plot)
  
  #Density
  intertime_density_plot <- ggplot(data = box.df, aes(in_session_intertime)) +
    geom_density(fill = "blue") +
    labs(title = paste("Time spent on each page, Mobile readers\n",datatype))
  ggsave(file = file.path(getwd(),"Data",paste(datatype,"intertime_density.png",sep = "_")),
         plot = intertime_density_plot)
  
  #Save quantiles
  write.table(x = quantile(in_session_intertime,seq(0,1,0.10)),
              file = file.path(getwd(),"Data",paste(datatype,"perpage_quantiles.tsv",sep = "_")),
              sep = "\t")
    
}