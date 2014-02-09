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
for(i in 1:nrow(data.df)){
  
  #Use MD5, as the least resource-intensive non-broken hashing algorithm available.
  data.df$IP[i] <- digest(object = paste(data.df$IP[i], data.df$lang[i], data.df$UA[i]),
                          algo = "md5")
}

#How many uniques do we have now?
named_vector[5] <- length(unique(data.df$IP))
names(named_vector)[5] <- "unique clients"

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