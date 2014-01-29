#Libraries
library(digest)

#Options
options(scipen = 500) #Eliminate (effectively) scientific notation
options(StringsAsFactors = FALSE) #Remove factorising

#Variables
mobile_file <- file.path(getwd(),"mobile_data.tsv") #Address of the hive output file
start_year <- 2014
start_month <- 1