#Function for reading the data in.
data_reader <- function(){
  
  #Stick in a while loop to control for Hive occasionally ganking out.
  while(file.exists(mobile_file) == FALSE){
    
    #Retrieve the IPs, save them to a table.
    system("hive --auxpath /usr/lib/hcatalog/share/hcatalog/hcatalog-core-0.5.0-cdh4.3.1.jar --database wmf -e '
           set hive.mapred.mode = nonstrict;
           INSERT OVERWRITE TABLE ironholds.distinct_ip
           SELECT dist_ip FROM (
           SELECT ip AS distip, COUNT(*) as count FROM wmf.webrequest_mobile WHERE year = 2014 AND month = 1 AND day BETWEEN 23 AND 30 AND cache_status = \'HIT\' AND http_status IN (\'\') AND content_type IN (\'text/html\; charset=utf-8\',\'text/html\; charset=iso-8859-1\',\'text/html\; charset=UTF-8','text/html\') GROUP BY ip HAVING COUNT(*) >= 2 ORDER BY rand())
           ) sub1 LIMIT 10000;'"
    )
    
    #retrieve the dataset as a whole, save it to file.
    system()
    
  }
  
  #Read in the file.
  data.df <- read.delim(file.path(getwd(),"Data","redeemed_data.tsv"),
                        as.is = TRUE,
                        header = TRUE,
                        quote = "",
                        col.names = c("timestamp",
                                      "IP",
                                      "URL_host",
                                      "URL_page",
                                      "URL_query",
                                      "referer",
                                      "lang",
                                      "UA"))
  
  #Concatenate URL
  data.df$URL_host <- paste0(data.df$URL_host,data.df$URL_page,data.df$URL_query)
  
  #Generate SHA-256 unique hashes
  hash <- character(nrow(data.df))
  
  for(i in seq_along(hash_vec)){
    
    hash_vec[i] <- digest(object = paste(data.df$IP[i],data.df$lang[i],data.df$UA[i]), algo = "sha256")
    
  }
  
  #Add the hash vector to the dataset, overwriting IP
  data.df$IP <- hash
  
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
              file = file.path(getwd(),"Data","metadata.tsv"),
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
    timestamps <- data.df$timestamp[data.df$hash == x,]
    
    #Order from earliest to latest
    timestamps <- timestamps[order(timestamps)]
    
    #Run through C++
    intervals <- intertime(x = timestamps)
    
    #Return
    return(intervals)
  }
  
  #Grab interval list
  intervals.ls <- lapply(x = as.list(unique(data.df$)), FUN = lapply_inter)
  
  #Save to an RData file for future screwin'-with.
  save(intervals.ls, file = file.path(getwd(),"Data","intervaldata.RData"))
  
  #Unlist and summarise
  aggregates.df <- as.data.frame(table(unlist(intervals.ls)))
}

