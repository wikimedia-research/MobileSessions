#Libraries
library(digest)
library(ggplot2)
library(Rcpp)
library(RMySQL)

#Cpp functions
sourceCpp("src/Intertime.cpp")
sourceCpp("src/Totaltime.cpp")
sourceCpp("src/Fromfirst.cpp")
sourceCpp("src/Sessionretriever.cpp")

#Options
options(scipen = 500) #Eliminate (effectively) scientific notation
options(StringsAsFactors = FALSE) #Remove factorising

#Files
mobile_file <- file.path(getwd(),"Data","mobile_data.tsv") #Address of the hive output file.
resulting_file <- file.path(getwd(),"Data","resulting_data.tsv") #Address of the save file.
metadata_file <- file.path(getwd(),"Data","metadata.tsv") #Metadata about the run

#MySQL variables
db_user <- "test"
db_pass <- "testpass"
server_list <- c("s1-analytics-slave.eqiad.wmnet",
                 "s2-analytics-slave.eqiad.wmnet",
                 "s3-analytics-slave.eqiad.wmnet",
                 "s4-analytics-slave.eqiad.wmnet",
                 "s5-analytics-slave.eqiad.wmnet",
                 "s6-analytics-slave.eqiad.wmnet",
                 "s7-analytics-slave.eqiad.wmnet") #List of MySQL servers

#Regex
bot_pattern = "(bot|crawl(er)?|http(s?):)"