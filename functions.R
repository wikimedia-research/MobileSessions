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

#MySQL functions
#The actual querying function
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

#Function for querying the db to get data about users
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

#Hashing function
hasher <- function(x){
  
  #Generate SHA-256 unique hashes
  hash_vec <- character(nrow(x))
  
  for(i in seq_along(hash_vec)){
    
    hash_vec[i] <- digest(object = paste(x$IP[i],x$lang[i],x$UA[i]), algo = "sha256")
    
  }
  
  #Add the hash vector to the dataset
  x$hash <- hash_vec
  
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
lapper <- function(dataset,func,filename){
  
  #Grab list
  results.ls <- lapply(X = as.list(unique(dataset$hash)), FUN = func, dataset = dataset)
  
  #Save to RData file for future screwin'-with.
  save(results.ls, file = filename)
  
  #Unlist and summarise
  aggregates.df <- as.data.frame(table(unlist(results.ls)))
  
  #Change type
  aggregates.df$Var1 <- as.numeric(as.character(aggregates.df$Var1))
  
  #Return
  return(aggregates.df)
  
}

grapher <- function(x, datatype){
  
  #Generate a log10 plot and save
  log10_plot <- ggplot(data = x, aes(log10(Freq))) +
    geom_area(stat = "bin", fill = "blue") +
    labs(title = paste("Log10 plot of time intervals\n(",datatype,")"),
         x = "Log10",
         y = "Number of occurrences")
  ggsave(file = file.path(getwd(),"Data",paste(datatype,"log10.png",sep = "_")),
         plot = log10_plot)
  
  #Smoothed plot
  smooth_plot <- ggplot(data = x, aes(Var1,Freq)) + 
    geom_smooth() + 
    labs(title = paste("Inter-time periods for mobile web requests, unlimited range\n(",datatype,")"),
         x = "Seconds",
         y = "Number of requests")
  ggsave(file = file.path(getwd(),"Data",paste(datatype,"smooth.png",sep = "_")),
         plot = smooth_plot)

  #Smoothed, limited plot
  limited_plot <- ggplot(data = x, aes(Var1,Freq)) + 
    geom_smooth() + 
    labs(title = paste("Inter-time periods for mobile web requests, limited range\n(",datatype,")"),
         x = "Seconds",
         y = "Number of requests") +
    scale_x_continuous(breaks = seq(0,3000,100), limits = c(0,3000))
ggsave(file = file.path(getwd(),"Data",paste(datatype,"limited_smooth.png",sep = "_")),
       plot = limited_plot)
  
}

#Post-minimum-identification analysis
sessionlength <- function(x, local_minimum){
  
  #Generate dataset
  results.ls <- lapply(X = as.list(unique(x$hash)), FUN = lapply_first, dataset = x)
  
  #Instantiate output object
  output.vec <- numeric(length(results.ls))
  
  for(i in seq_along(output.vec)){
    
    #Retrieve 'session time' and add it to the output vector.
    output.vec[i] <- totaltime(x = results.ls[[i]], local_minimum = local_minimum)
    
  }
  
  #Remove zero entries (entries where all datapoints were above the local minimum)
  output.vec <- output.vec[!output.vec == 0]
  
  #Dataframe it for ggplot2
  output.df <- as.data.frame(output.vec)
  
  totaltime_density <- ggplot(output.df,aes(output.vec)) + 
    geom_density(fill = "blue") +
    labs(title = "Total mobile session times",
         x = "Seconds",
         y = "Density") +
    guides(fill=FALSE)
  ggsave(file = file.path(getwd(),"Data","density.png"),
         plot = totaltime_density)
  
}