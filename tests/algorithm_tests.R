#Tests for the hypotheses underlying the algorithms we're using to compute unique client and session time.
algorithm_tests <- function(){
  
  #Libraries and sourcing
  source("config.R")
  source("functions.R")
  
  #Retrieve data
  system("hive --auxpath /usr/lib/hcatalog/share/hcatalog/hcatalog-core-0.5.0-cdh4.3.1.jar --database wmf -e \"
         set hive.mapred.mode = nonstrict;
         INSERT INTO TABLE ironholds.test_ip
         SELECT distip FROM (
         SELECT ip AS distip, COUNT(*) as count FROM wmf.webrequest_mobile WHERE year = 2014 AND month = 1 AND day = 20 GROUP BY ip ORDER BY rand()
         ) sub1 WHERE count >= 2 LIMIT 10000;\""
  )
  
  system(paste("hive --auxpath /usr/lib/hcatalog/share/hcatalog/hcatalog-core-0.5.0-cdh4.3.1.jar --database wmf -e \"
         SELECT
          db1.dt,
          db1.ip,
          db1.cache_status,
          db1.http_status,
          db1.uri_host,
          db1.uri_path,
          db1.uri_query,
          db1.referer,
          db1.accept_language,
          db1.content_type,
          db1.user_agent
        FROM wmf.webrequest_mobile db1 INNER JOIN ironholds.distinct_ip db2 ON db1.ip = db2.ip
        WHERE db1.year = 2014 AND db1.month = 1 AND db1.day = 20;\" >",resulting_file))
  
  #Work out the degrees of entropy this gives us
  entropy <- function(x){
    
    #Instantiate output object
    entropy.vec <- numeric(13)
    
    #Sample IPs
    entropy.vec[1] <- length(unique(x$IP))
    names(entropy.vec)[1] <- "Unique IPs"
    
    #IP + UA
    test.vec <- character(nrow(x))
    for(i in seq_along(test.vec)){
      
      test.vec[i] <- digest(object = paste(x$IP[i],x$UA[i]), algo = "sha256")
      
    }
    entropy.vec[2] <- length(unique(test.vec))
    names(entropy.vec)[2] <- "Unique IP+UA"
    
    #IP + UA + lang
    for(i in seq_along(test.vec)){
      
      test.vec[i] <- digest(object = paste(x$IP[i],x$UA[i],x$lang[i]), algo = "sha256")
      
    }
    entropy.vec[3] <- length(unique(test.vec))
    names(entropy.vec)[3] <- "Unique IP+UA+lang"
    
    #Compare to desktop data
    sampled.df <- trickstr::samplelogs("/a/squid/archive/sampled/sampled-1000.tsv.log-20140120.gz")
    sampled.df <- sampled.df[!grepl(x = sampled.df$URL, pattern = "\\.m\\.wiki", ignore.case = TRUE),]
    sampled.df <- sampled.df[sampled.df$IP %in% sample(sampled.df$IP,10000),]
    entropy.vec[4] <- length(unique(sampled.df$IP))
    names(entropy.vec)[4] <- "sampled desktop IPs"
    sampled.df <- hasher(x = sampled.df)
    entropy.vec[5] <- length(unique(sampled.df$IP))
    names(entropy.vec)[5] <- "Desktop, IP+UA+lang"
    
    #Compare to mobile editors
    mobile_edits.df <- globalsql("SELECT cuc_timestamp AS timestamp, cuc_user_text AS user, cuc_ip AS IP, cuc_agent AS UA FROM cu_changes INNER JOIN revision ON cuc_this_oldid = rev_id INNER JOIN tag_summary ON ts_rev_id = rev_id WHERE rev_timestamp BETWEEN '20140220000000' AND '20140220235959' AND ts_tags LIKE '%mobile edit%' AND rev_parent_id > 0 AND rev_user_text NOT LIKE '%bot%';")
    
    #Restrict to the first session, based on halfak's session length definition. Let's use 430 seconds.
    mobile_edits.df$timestamp <- as.numeric(strptime(x = mobile_edits.df$timestamp, format = "%Y%m%d%H%M%S"))
    mobile_edits.df <- do.call("rbind",lapply(unique(mobile_edits.df$user),function(x){
      
      #Retrieve dataset, identify offsets, identify offsets within 430 seconds
      valid_rows <- sessionretriever(x = intertime(x = mobile_edits.df$timestamp[mobile_edits.df$user == x]),
                                     local_minimum = 430)
      
      #Return the relevant rows
      return(mobile_edits.df[mobile_edits.df$user == x,][1:valid_rows,])
    }))
    
    #Checks
    entropy.vec[6] <- length(unique(mobile_edits.df$IP))
    names(entropy.vec)[6] <- "Unique mobile editor IPs"
    entropy.vec[7] <- length(unique(mobile_edits.df$user))
    names(entropy.vec)[7] <- "Unique mobile editors"
    
    #Too small. Rats. Module storage it is.
    storage.df <- rawsql(statement = "SELECT event_experimentId AS userID,
                         timestamp,
                         clientIP AS IP,
                         event_userAgent AS UA
                         FROM ModuleStorage_6978194
                         WHERE left(timestamp,8) = '20140120'
                         AND event_mobileMode IN ('stable','beta','alpha');",
                         db_host = "s1-analytics-slave.eqiad.wmnet",
                         db_database = "log")
    
    #Checks
    entropy.vec[8] <- length(unique(storage.df$IP))
    names(entropy.vec)[8] <- "unique ModuleStorage IPs"
    entropy.vec[8] <- length(unique(storage.df$userID))
    names(entropy.vec)[8] <- "unique ModuleStorage users"
    entropy.vec[8] <- nrow(storage.df)
    names(entropy.vec)[8] <- "ModuleStorage entries"
    
    #Work out IP:editor ratio, and vice versa
    ip_to_editor <- unlist(lapply(unique(storage.df$IP),function(x){
      
      return(length(unique(storage.df$userID[storage.df$IP == x])))
      
    }))
    
    editor_to_ip <- unlist(lapply(unique(storage.df$userID),function(x){
      
      return(length(unique(storage.df$IP[storage.df$userID == x])))
      
    }))
    
    entropy.vec[10] <- length(ip_to_editor[ip_to_editor > 1])
    names(entropy.vec)[10] <- "IPs with multiple UUIDs"
    entropy.vec[10] <- length(editor_to_ip[editor_to_ip > 1])
    names(entropy.vec)[11] <- "UUIDs with multiple IPs"
    
    #Plot
    ip_density <- ggplot(as.data.frame(ip_to_editor),aes(ip_to_editor)) +
      geom_density(fill = "blue") +
      labs(title = "IPs with associated UUIDs in ModuleStorage",
           x = "Number of associated users")
    ggsave(filename = file.path(getwd(),"Data","TestingData","ip_to_users.png"),
           plot = ip_density)
    user_density <- ggplot(as.data.frame(editor_to_ip), aes(editor_to_ip)) +
      geom_density(fill = "blue") +
      labs(title = "UUIDs with associated IPs in ModuleStorage",
           x = "Number of associated IPs")
    ggsave(filename = file.path(getwd(),"Data","TestingData","users_to_ip.png"),
           plot = user_density)
    
    #Restrict to sessions
    #Format timestamps
    storage.df$timestamp <- as.numeric(strptime(x = storage.df$timestamp, format = "%Y%m%d%H%M%S"))
    
    #Test session length
    storage.df$hash <- storage.df$userID
    
    #Work out the from-first values
    fromfirst <- lapper(storage.df,lapply_first,file.path(getwd(),"Data","TestingData","storage_fromfirst.RData"))
    intertime <- lapper(storage.df,lapply_inter,file.path(getwd(),"Data","TestingData","storage_fromfirst.RData"))

    #Plot
    storage_fromfirst <- ggplot(data = fromfirst, aes(Var1,Freq)) + 
      geom_smooth() + 
      labs(title = "Period from first requests to each subsequent request, ModuleStorage",
           x = "Seconds",
           y = "Number of requests")
    ggsave(filename = file.path(getwd(),"Data","TestingData","storage_from_first.png"),
           plot = storage_fromfirst)
    storage_intertime <- ggplot(data = intertime, aes(Var1,Freq)) + 
      geom_smooth() + 
      labs(title = "Inter-time periods, ModuleStorage",
           x = "Seconds",
           y = "Number of requests")
    ggsave(filename = file.path(getwd(),"Data","TestingData","storage_intertime.png"),
           plot = storage_intertime)
    
    #Restricted sets
    storage_fromfirst_restricted <- ggplot(data = fromfirst[fromfirst$Var1 <= 3000,], aes(Var1,Freq)) + 
      geom_smooth() + 
      labs(title = "Period from first requests to each subsequent request, ModuleStorage\nlimited dataset",
           x = "Seconds",
           y = "Number of requests")
    ggsave(filename = file.path(getwd(),"Data","TestingData","storage_from_first_restricted.png"),
           plot = storage_fromfirst_restricted)
    storage_intertime_restricted <- ggplot(data = intertime[intertime$Var1 <= 3000,], aes(Var1,Freq)) + 
      geom_smooth() + 
      labs(title = "Inter-time periods, ModuleStorage\nlimited dataset",
           x = "Seconds",
           y = "Number of requests")
    ggsave(filename = file.path(getwd(),"Data","TestingData","storage_intertime_restricted.png"),
           plot = storage_intertime_restricted)
    
    #Informed by this local minimum...
    storage.df <- do.call("rbind",lapply(unique(storage.df$userID),function(x){
      
      #Retrieve dataset, identify offsets, identify offsets within 430 seconds
      valid_rows <- sessionretriever(x = intertime(x = storage.df$timestamp[storage.df$userID == x]),
                                     local_minimum = 430)
      
      #Return the relevant rows
      return(storage.df[storage.df$userID == x,][1:valid_rows,])
    }))
    
    #Run again
    session_editor_to_ip <- unlist(lapply(unique(storage.df$userID),function(x){
      
      return(length(unique(storage.df$IP[storage.df$userID == x])))
      
    }))
    
    entropy.vec[11] <- length(session_editor_to_ip[session_editor_to_ip > 1])
    names(entropy.vec)[11] <- "UUIDs with multiple IPs (first session)"
    
    #Hash
    hash.vec <- character(nrow(storage.df))
    for(i in seq_along(hash.vec)){
      
      hash.vec[i] <- digest(object = paste(storage.df$IP[i],storage.df$UA[i]), algo = "sha256")
    }
    
    #Add
    storage.df$hash <- hash.vec
    
    #Run again
    hash_to_UUID <- unlist(lapply(unique(storage.df$hash),function(x){
      
      return(length(unique(storage.df$userID[storage.df$hash == x])))
      
    }))
    userID_to_hash <- unlist(lapply(unique(storage.df$userID),function(x){
      
      return(length(unique(storage.df$hash[storage.df$userID == x])))
      
    }))
    entropy.vec[12] <- length(hash_to_UUID)
    names(entropy.vec)[12] <- "Hashes"
    entropy.vec[13] <- length(hash_to_UUID[hash_to_UUID > 1])
    names(entropy.vec)[13] <- "Hashes with more than one distinct UUID associated"
    
    #Save
    write.table(x = entropy.vec,
                file = file.path(getwd(),"Data","TestingData","entropy_data.tsv"))
  }
  
  #Dataloss and how things break down.
  dataloss <- function(x){
    
    #Instantiate output object
    dataloss.vec <- numeric(6)
    
    #How many entries do we start with?
    dataloss.vec[1] <- nrow(x)
    names(dataloss.vec)[1] <- "requestlog entries"
    
    #How does it break down by MIME type?
    mime_plot <- ggplot(data = as.data.frame(table(x$MIME_type)), aes(x = Var1, y = Freq)) +
      geom_bar(stat = "identity") +
      theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
      labs(title = "Frequency of MIME types in Mobile requests\n(sample of 162,238 pages from 10,000 IPs",
           x = "MIME type",
           y = "Number of requests")
    ggsave(file = file.path(getwd(),"Data","TestingData","MIME_types.png"),
           plot = mime_plot)
    
    #How many referrers do we lose?
    dataloss.vec[2] <- nrow(x[x$referrer == "-",])
    names(dataloss.vec)[2] <- "Lost referers"
    
    #For 'actual' pageviews?
    dataloss.vec[3] <- nrow(x[x$referrer == "-" & x$MIME_type %in% c("text/html; charset=utf-8",
                                                                     "text/html; charset=iso-8859-1",
                                                                     "text/html; charset=UTF-8",
                                                                     "text/html"),])
    names(dataloss.vec)[3] <- "Lost referers in 'actual' pageviews"
    
    #Of...
    dataloss.vec[4] <- nrow(x[x$MIME_type %in% c("text/html; charset=utf-8",
                                                                     "text/html; charset=iso-8859-1",
                                                                     "text/html; charset=UTF-8",
                                                                     "text/html"),])
    names(dataloss.vec)[4] <- "'actual' pageviews"
    
    #For Special:BannerRandom hits?
    dataloss.vec[5] <- nrow(x[x$referrer == "-" & x$URL_page == "/wiki/Special:BannerRandom",])
    names(dataloss.vec)[5] <- "Lost referers in banner pageviews"
    dataloss.vec[6] <- nrow(x[x$URL_page == "/wiki/Special:BannerRandom",])
    names(dataloss.vec)[6] <- "banner pageviews"
    
    #Save
    write.table(x = dataloss.vec,
                file = file.path(getwd(),"Data","TestingData","dataloss.tsv"))
  }
  
  #Session algorithm testing
  sessions <- function(x){
    
    #Hash
    data.df <- hasher(x = x)
    
    #Format timestamps as value in seconds
    data.df$timestamp <- as.numeric(strptime(x = data.df$timestamp, format = "%Y-%m-%dT%H:%M:%S"))
    
    #Work out the from-first values
    fromfirst <- lapper(data.df,lapply_first,file.path(getwd(),"Data","TestingData","fromfirst.RData"))
    
    #Work out the interval values
    intervals <- lapper(data.df,lapply_inter,file.path(getwd(),"Data","TestingData","intertime.RData"))
    
  }
  
  #Read it in
  data.df <- read.delim(file = resulting_file,
                        as.is = TRUE,
                        header = TRUE,
                        quote = "",
                        col.names = c("timestamp",
                                      "IP",
                                      "Cache_status",
                                      "Host_status",
                                      "URL_host",
                                      "URL_page",
                                      "URL_query",
                                      "referrer",
                                      "lang",
                                      "MIME_type",
                                      "UA"))
  
  #Remove null entries (we really need to be escaping/removing tabs in UAs. Safari Mobile 4.0, I'm looking at you)
  data.df <- data.df[!data.df$IP == "",]
  
  #Work out entropy, data loss and sessions
  entropy(data.df)
  dataloss(data.df)
  sessions(data.df[data.df$MIME_type %in% c("text/html; charset=utf-8",
                                            "text/html; charset=iso-8859-1",
                                            "text/html; charset=UTF-8",
                                            "text/html"),])
}

#Run
algorithm_tests()