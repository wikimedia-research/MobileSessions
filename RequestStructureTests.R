#Queries used to retrieve this data:
#INSERT OVERWRITE TABLE ironholds.distinct_ip SELECT DISTINCT(ip) FROM wmf.webrequest_mobile WHERE year = 2014 AND month = 1 AND day = 20 LIMIT 10000;
#SELECT db1.dt,
#db1.ip,
#db1.cache_status,
#db1.host_status,
#db1.uri_host,
#db1.uri_path,
#db1.referer,
#db1.accept_language,
#db1.content_type,
#db1.user_agent
#FROM wmf.webrequest_mobile db1 INNER JOIN ironholds.distinct_ip db2 ON db1.ip = db2.ip WHERE db1.year = 2014 AND db1.month = 1 AND db1.day = 20;

#Options
options(scipen = 500) #Eliminate (effectively) scientific notation
options(StringsAsFactors = FALSE) #Remove factorising

#Libaries
library(digest) #Hash generation
library(ggplot2) #Plotting
library(plyr) #Generating intervals

#Read in the test data
data.df <- read.delim(file.path(getwd(),"Data","redeemed_data.tsv"),
                      as.is = TRUE,
                      header = TRUE,
                      quote = "",
                      col.names = c("timestamp",
                                    "IP",
                                    "Cache_status",
                                    "Host_status",
                                    "URL_host",
                                    "URL_page",
                                    "referrer",
                                    "lang",
                                    "MIME_type",
                                    "UA"))

#Remove null entries (we really need to be escaping/removing tabs in UAs. Safari Mobile 4.0, I'm looking at you)
data.df <- data.df[!data.df$IP == "",]

#Initialise named vector
named_vector <- numeric(10)

#How many entries do we start with?
named_vector[1] <- nrow(data.df)
names(named_vector)[1] <- "requestlog entries"

#After filtering cache_status == "MISS"?
data.df <- data.df[data.df$Cache_status == "hit",] #Eliminate non-hits
named_vector[2] <- nrow(data.df)
names(named_vector)[2] <- "requestlog entries minus varnish misses"

#After filtering for HTTP status codes?
data.df <- data.df[data.df$Host_status %in% c(200,304,302,301),] #Eliminate bad requests
named_vector[3] <- nrow(data.df)
names(named_vector)[3] <- "requestlog entries minus invalid http codes"

#How many referrers do we lose?
named_vector[4] <- nrow(data.df[data.df$referrer == "-",])
names(named_vector)[4] <- "Lost referers"

#How many IPs do we start with?
named_vector[5] <- length(unique(data.df$IP))
names(named_vector)[5] <- "unique IPs"

#Hash. Unfortunately digest() is not vectorised; I may implement it in a vectorised way if I get bored.
#I can still get some speed improvements by using a vector, mind.
sha2vec <- character(nrow(data.df))

for(i in 1:nrow(data.df)){
  
  #Use SHA256, as the least resource-intensive collision resistant algorithm I have access to.
  sha2vec[i] <- digest(object = paste(data.df$IP[i], data.df$lang[i], data.df$UA[i]),
                          algo = "sha256")
}

#Add vector to data.df
data.df$IP <- sha2vec

#How many uniques do we have now?
named_vector[6] <- length(unique(data.df$IP))
names(named_vector)[6] <- "unique clients"

#Plot MIME types
mime_plot <- ggplot(data = as.data.frame(table(data.df$MIME_type)), aes(x = Var1, y = Freq)) +
                      geom_bar(stat = "identity") + 
                      theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
                      labs(title = "Frequency of MIME types in Mobile requests\n(sample of 162,238 pages from 10,000 IPs",
                           x = "MIME type",
                           y = "Number of requests")

#Save
ggsave(file = file.path(getwd(),"Data","TestingData","MIME_types.png"),
       plot = mime_plot)

#Save a table version of this data just to be save
write.table(x = as.data.frame(table(data.df$MIME_type)),
            file = file.path(getwd(),"Data","TestingData","MIME_types.tsv"),
            row.names = FALSE,
            col.names = TRUE)

#How many pageviews do we have with 'actual' MIME types?
data.df <- data.df[data.df$MIME_type %in% c("text/html; charset=utf-8",
                                            "text/html; charset=iso-8859-1",
                                            "text/html; charset=UTF-8",
                                            "text/html"),]
named_vector[7] <- nrow(data.df)
names(named_vector)[7] <- "'actual' pageviews"

#How much referer loss is there?
named_vector[8] <- nrow(data.df[data.df$referrer == "-",])
names(named_vector)[8] <- "referer loss in actual pageviews"

#Format the timestamp as a value in seconds.
data.df$timestamp <- as.numeric(strptime(x = data.df$timestamp, format = "%Y-%m-%dT%H:%M:%S"))

#Check out how the 'mae west curve' hypothesis of session times works.
maewest.vec <- unlist(lapply(X = unique(data.df$IP), FUN = function(x){
  
  #Instantiate initial dataset
  dataset <- data.df[data.df$IP == x,]$timestamp
  
  #Order the incoming dataset from earliest to latest
  dataset <- dataset[order(dataset)]
  
  #Instantiate object
  to_return.vec <- numeric(length(dataset) - 1)
  
  
  #Looping through, work out the setoff from the first pageview to the last.
  for(i in seq_along(dataset)){
    
    if(i > 1){
      
      to_return.vec[i-1] <- dataset[i] - dataset[1]
      
    }
  }
  
  return(to_return.vec)
  
}))

#Generate quantiles
quantiles <- quantile(maewest.vec, probs = seq(0,1,0.05))

#Save quantiles
write.table(x = as.data.frame(quantiles),
            file = file.path(getwd(),"Data","TestingData","pageview_quantiles.tsv"),
            row.names = TRUE,
            col.names = TRUE)

#Compress
mae.df <- as.data.frame(table(maewest.vec))

#Generate a log10 plot and save
log10_plot <- ggplot(data = mae.df, aes(log10(Freq))) +
  geom_area(stat = "bin", fill = "blue") +
  labs(title = "Log10 plot of time elapsed between a 'first' pageview and each successive pageview",
       x = "Log10",
       y = "Number of occurrences")

ggsave(file = file.path(getwd(),"Data","TestingData","First_log10.png"),
       plot = log10_plot)

#What does the data in the bottom 30% look like?
mae_30.df <- as.data.frame(table(maewest.vec[maewest.vec <= quantiles[names(quantiles) == "30%"]]))

#Numericise
mae_30.df$Var1 <- as.numeric(as.character(mae_30.df$Var1))

#Plot
plot_30 <- ggplot(data = mae_30.df, aes(Var1,Freq)) + 
  geom_area() + 
  labs(title = "Frequency of page requests, in seconds, after the 'first' request",
       x = "Seconds",
       y = "Number of requests")

smoothed_plot <- ggplot(data = mae_30.df, aes(Var1,Freq)) + 
  geom_smooth() + 
  labs(title = "Frequency of page requests, in seconds, after the 'first' request\n with smoothing",
       x = "Seconds",
       y = "Number of requests")

#Save
ggsave(file = file.path(getwd(),"Data","TestingData","Session_bottom_30.png"),
       plot = plot_30)
ggsave(file = file.path(getwd(),"Data","TestingData","Session_bottom_smoothed.png"),
       plot = smoothed_plot)

#What if we look at difference between reads, too?
between_reads.vec <- unlist(lapply(X = unique(data.df$IP), FUN = function(x){
  
  #Instantiate initial dataset
  dataset <- data.df[data.df$IP == x,]$timestamp
  
  #Order the incoming dataset from earliest to latest
  dataset <- dataset[order(dataset)]
  
  #Instantiate object
  to_return.vec <- numeric(length(dataset) - 1)
  
  
  #Looping through, work out the setoff from the first pageview to the last.
  for(i in seq_along(dataset)){
    
    if(i > 1){
      
      to_return.vec[i-1] <- dataset[i] - dataset[i-1]
      
    }
  }
  
  return(to_return.vec)
  
}))
between_reads.df <- as.data.frame(table(between_reads.vec))

between_log10_plot <- ggplot(data = as.data.frame(table(between_reads.vec)), aes(log10(Freq))) +
  geom_area(stat = "bin", fill = "blue") +
  labs(title = "Log10 plot of time elapsed between each mobile pageview",
       x = "Log10",
       y = "Number of occurrences")

ggsave(file = file.path(getwd(),"Data","TestingData","Session_log10.png"),
       plot = log10_plot)

#Generate quantiles
between_quants <- quantile(between_reads.vec, probs = seq(0,1,0.05))

#Save quantiles
write.table(x = as.data.frame(between_quants),
            file = file.path(getwd(),"Data","TestingData","between_quantiles.tsv"),
            row.names = TRUE,
            col.names = TRUE)