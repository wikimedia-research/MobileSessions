#Libraries
library(digest)
library(ggplot2)
library(Rcpp)

#Options
options(scipen = 500) #Eliminate (effectively) scientific notation
options(StringsAsFactors = FALSE) #Remove factorising

#Files
mobile_file <- file.path(getwd(),"Data","mobile_data.tsv") #Address of the hive output file.
resulting_file <- file.path(getwd(),"Data","resulting_data.tsv") #Address of the save file.
metadata_file <- file.path(getwd(),"Data","metadata.tsv") #Metadata about the run