#Queries used to retrieve this data:
#INSERT OVERWRITE TABLE ironholds.distinct_ip SELECT DISTINCT(ip) FROM wmf.webrequest_mobile WHERE year = 2014 AND month = 1 AND day = 20 LIMIT 10000;
#SELECT db1.dt,
#db1.ip,
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
                                    "URL_host",
                                    "URL_page",
                                    "referrer",
                                    "lang",
                                    "MIME_type",
                                    "UA"))

#Scope the plotting
plotting.fun <- function(){
}