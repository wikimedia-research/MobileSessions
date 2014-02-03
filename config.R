#Libraries
library(digest)

#Options
options(scipen = 500) #Eliminate (effectively) scientific notation
options(StringsAsFactors = FALSE) #Remove factorising

#Variables
start_year <- 2014
start_month <- 1

#Files
mobile_file <- file.path(getwd(),"mobile_data.tsv") #Address of the hive output file.
resulting_file <- file.path(getwd(),"resulting_data.tsv") #Address of the save file.
metadata_file <- file.path(getwd(),"metadata.tsv") #Metadata about the run