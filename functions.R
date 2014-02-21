#Function for reading the data in.
data_reader <- function(){
  
  #Stick in a while loop to control for Hive occasionally ganking out.#
  #I really, really need to build that hadoop/RHive vagrant instance and convince Ops we need that.
  while(file.exists(mobile_file) == FALSE){
    
    #Retrieve the IPs, save them to a table.
    system("hive --auxpath /usr/lib/hcatalog/share/hcatalog/hcatalog-core-0.5.0-cdh4.3.1.jar --database wmf -e \"
           set hive.mapred.mode = nonstrict;
           INSERT OVERWRITE TABLE ironholds.distinct_ip
           SELECT distip FROM (
           SELECT ip AS distip, COUNT(*) as count FROM wmf.webrequest_mobile WHERE year = 2014 AND month = 1 AND day BETWEEN 23 AND 30 AND cache_status = \'hit\' AND http_status IN (\'200\',\'301\',\'302\',\'304\') AND content_type IN (\'text/html\\; charset=utf-8\',\'text/html\\; charset=iso-8859-1\',\'text/html\\; charset=UTF-8','text/html\') GROUP BY ip ORDER BY rand()
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
          WHERE db1.year = 2014 AND db1.month = 1 AND db1.day BETWEEN 23 AND 30 AND db1.cache_status = \'hit\'
          AND db1.http_status IN (200,301,302,304) AND db1.content_type IN (\'text/html\\; charset=utf-8\',\'text/html\\; charset=iso-8859-1\',\'text/html\\; charset=UTF-8','text/html\');\" >",mobile_file))
  }
  
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
  
  #Concatenate URL
  data.df$URL_host <- paste(data.df$URL_host,data.df$URL_page,data.df$URL_query,sep = "")
  
  #Generate SHA-256 unique hashes
  hash_vec <- character(nrow(data.df))
  
  for(i in seq_along(hash_vec)){
    
    hash_vec[i] <- digest(object = paste(data.df$IP[i],data.df$lang[i],data.df$UA[i]), algo = "sha256")
    
  }
  
  #Add the hash vector to the dataset, overwriting IP
  data.df$IP <- hash_vec
  
  #Limit to those hashes with >1 article view
  data.df <- data.df[data.df$IP %in% subset(as.data.frame(table(data.df$IP)), Freq > 1)$Var1,]
  
  #Convert timestamps to seconds
  data.df$timestamp <- as.numeric(strptime(x = data.df$timestamp, format = "%Y-%m-%dT%H:%M:%S"))
  
  #Strip unnecessary columns and rename
  data.df <- data.df[,c("timestamp","IP","URL_host","referer")]
  names(data.df) <- c("timestamp","hash","URL","referer")
  
  #Return
  return(data.df)
}

#Function for extracting and logging metadata
logger <- function(){
  
  #Instantiate metadata object
  metadata.vec <- numeric(3)
  
  #How many unique clients do we have?
  metadata.vec[1] <- length(data.df$hash)
  names(metadata.vec)[i] <- "Unique client hashes"
  
  #How many pageviews does that come to?
  metadata.vec[2] <- nrow(data.df)
  names(metadata.vec)[2] <- "pageviews"
  
  #How many have lost referrers?
  metadata.vec[3] <- nrow(data.df[data.df$referer == "-",])
  names(metadata.vec)[3] <- "pageviews with no referer"
  
  #Write to file
  write.table(x = metadata.vec,
              file = metadata_file,
              quote = TRUE,
              sep = "\t",
              row.names = TRUE,
              col.names = FALSE)
}

#Analysis function
basic_analysis <- function(){
  
  #Intertime function
  lapply_inter <- function(x){
    
    #Split out timestamps
    timestamps <- data.df$timestamp[data.df$hash == x]
    
    #Run through C++
    intervals <- intertime(x = timestamps)
    
    #Return
    return(intervals)
  }
  
  #From-first function
  lapply_first <- function(x){
    
    #Split out timestamps
    timestamps <- data.df$timestamp[data.df$hash == x]
    
    #Run through C++
    first_intervals <- fromfirst(x = timestamps)
    
    #Return
    return(first_intervals)
    
  }
  
  #Grab interval list and from-first list
  intervals.ls <- lapply(X = as.list(unique(data.df$hash)), FUN = lapply_inter)
  
  #Save to RData file for future screwin'-with.
  save(intervals.ls, file = file.path(getwd(),"Data","intervaldata.RData"))
  
  #Unlist and summarise
  aggregates.df <- as.data.frame(table(unlist(intervals.ls)))
  
  #Change type
  aggregates.df$Var1 <- as.numeric(as.character(aggregates.df$Var1))
  
  #Generate a log10 plot and save
  log10_plot <- ggplot(data = aggregates.df, aes(log10(Freq))) +
    geom_area(stat = "bin", fill = "blue") +
    labs(title = "Log10 plot of inter-time periods between mobile web requests",
         x = "Log10",
         y = "Number of occurrences")
  ggsave(file = file.path(getwd(),"Data","log10.png"),
         plot = log10_plot)
  
  #Smoothed plot
  smooth_plot <- ggplot(data = aggregates.df, aes(Var1,Freq)) + 
    geom_smooth() + 
    labs(title = "Inter-time period between mobile web requests\nunlimited range",
         x = "Seconds",
         y = "Number of requests")
  ggsave(file = file.path(getwd(),"Data","smooth_plot.png"),
         plot = smooth_plot)
  
  #Smoothed, limited plot
  limited_plot <- ggplot(data = aggregates.df, aes(Var1,Freq)) + 
    geom_smooth() + 
    labs(title = "Inter-time period between mobile web requests\nlimited range (0th to 10th percentile)",
         x = "Seconds",
         y = "Number of requests") +
    scale_x_continuous(breaks = seq(0,3000,100), limits = c(0,3000))
  ggsave(file = file.path(getwd(),"Data","limited_smoothed.png"),
         plot = limited_plot)
  
  #Grab from-first list
  fromfirst.ls <- lapply(X = as.list(unique(data.df$hash)), FUN = lapply_first)
  
  #Save
  save(fromfirst.ls, file = file.path(getwd(),"Data","fromfirst.RData"))
  
  #Unlist and summarise
  from_aggs.df <- as.data.frame(table(unlist(fromfirst.ls)))
  
  #Numericise
  from_aggs.df$Var1 <- as.numeric(as.character(from_aggs.df$Var1))
  
  #Generate a log10 plot and save
  from_log10_plot <- ggplot(data = from_aggs.df, aes(log10(Freq))) +
    geom_area(stat = "bin", fill = "blue") +
    labs(title = "Log10 plot of time elapsed from first mobile web request",
         x = "Log10",
         y = "Number of occurrences")
  ggsave(file = file.path(getwd(),"Data","from_log10.png"),
         plot = from_log10_plot)
  
  #Smoothed plot
  from_smooth_plot <- ggplot(data = from_aggs.df, aes(Var1,Freq)) + 
    geom_smooth() + 
    labs(title = "Time elapsed from first mobile web request\nunlimited range",
         x = "Seconds",
         y = "Number of requests")
  ggsave(file = file.path(getwd(),"Data","from_smooth_plot.png"),
         plot = from_smooth_plot)
  
  #Smoothed, limited plot
  from_limited_plot <- ggplot(data = from_aggs.df, aes(Var1,Freq)) + 
    geom_smooth() + 
    labs(title = "Time elapsed from first mobile web request\nlimited range (0th to 10th percentile)",
         x = "Seconds",
         y = "Number of requests") +
    scale_x_continuous(breaks = seq(0,3000,100), limits = c(0,3000))
  ggsave(file = file.path(getwd(),"Data","from_limited_smoothed.png"),
         plot = from_limited_plot)
  
}

#Post-minimum-identification analysis
post_min_analysis <- function(){
  
  #We saw 400-500 (450) as the local minimum. Cool!
  #Instantiate output object
  output.vec <- numeric(length(fromfirst.ls))
  
  for(i in seq_along(fromfirst.ls)){
    
    #Retrieve 'session time' and add it to the output vector.
    output.vec[i] <- totaltime(x = fromfirst.ls[[i]], local_minimum = 450)
    
  }
  
}