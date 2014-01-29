MobileSessions <- function(){
  
  #Grab the data from hive
  query_log <- system(paste("hive --auxpath /usr/lib/hcatalog/share/hcatalog/hcatalog-core-0.5.0-cdh4.3.1.jar --database wmf -e '
                SELECT dt,
                  ip,
                  uri_host,
                  uri_path,
                  uri_query,
                  content_type,
                  referer,
                  user_agent
                FROM webrequest_mobile 
                WHERE year >= ",start_year,"
                  AND month >= ",start_month,"' > ",mobile_file, sep = ""))
  
  #After the data _has_ been grabbed, read it in
  data.df <- read_data(mobile_file)
  
  
  
}