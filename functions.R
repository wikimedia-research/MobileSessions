#Function for reading data
read_data <- function(day){
  
  #Grab the data from hive
  query_log <- system(paste('hive --auxpath /usr/lib/hcatalog/share/hcatalog/hcatalog-core-0.5.0-cdh4.3.1.jar --database wmf -e "
                SELECT dt,
                  ip,
                  uri_host,
                  uri_path,
                  referer,
                  accept_language,
                  user_agent
                FROM webrequest_mobile 
                WHERE year >= ',start_year,'
                  AND month >= ',start_month,'
                  AND uri_path LIKE \'%Special:BannerRandom%\'
                  AND referer != \'-\'
                  AND day =',day,';"> ',mobile_file, sep = ""))
  
  #Read in the data
  data.df <- read.delim(file = mobile_file,
                        as.is = TRUE,
                        header = TRUE,
                        quote = "",
                        col.names = c("timestamp",
                                      "IP",
                                      "URL_host",
                                      "URL_page",
                                      "referrer",
                                      "lang",
                                      "UA"))
  
  #Kill the file to save space
  file.remove(mobile_file)
  
  #Remove invalid entries
  data.df <- data.df[!data.df$referrer == "-" | !data.df$UA == "-",]
  
  #Note number of IPs
  IP_num <- length(unique(data.df$IP))
  
  #Randomly sample on an IP basis. Let's use 10,000
  data.df <- data.df[data.df$IP %in% sample(unique(data.df$IP, 10000)),]
  
  #Hash IP, language and UA to come up with a (terrible) proxy for unique users.
  data.df$unique_hash <- digest(x = paste(data.df$IP,data.df$lang, data.df$UA),
                                algo = "md5")
  
  #Return both the data and the number of unique IPs
  return(list(data.df,
              IP_num))
  
}

#Function that wraps around read_data to retrieve 20 days of info
DataRetrieve <- function(){
  
  #Instantiate object to return
  resulting.df <- data.frame()
  
  #Assume 20 days
  unique_IPs <- numeric(20)
  total_IPs <- numeric(20)
  rows <- numeric(20)
  day <- numeric(20)
  
  for(i in seq_along(20)){
    
    #Grab the data
    data.ls <- read_data(date = i)
    
    #Note unique IPs
    unique_IPs[i] <- length(unique(data.ls[[1]]$IP))
    
    #Note total unique IPs
    total_IPs[i] <- data.df[[2]]
    
    #Note number of entries
    rows[i] <- nrow(data.ls[[1]])
    
    #Note day
    day[i] <- i
    
    #Sanitise data and save it
    resulting.df <- cbind(resulting.df,data.ls[[1]][,c("timestamp","referrer","unique_hash")])
    
  }
  
  #Save the resulting data frame
  write.table(x = resulting.df,
              file = resulting_file,
              quote = FALSE
              sep = "\t"
              row.names = FALSE)
  
  #Bind metadata into a data frame and save that, too.
  metadata.df <- data.frame(unique_IPs,total_IPs,rows,day)
  write.table(x = metadata.df,
              file = metadata_file,
              quote = FALSE
              sep = "\t"
              row.names = FALSE)
  
  #After the data _has_ been grabbed, read it in
  data.df <- read_data(mobile_file)
}