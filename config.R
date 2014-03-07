# Copyright (c) 2014 Oliver Keyes
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
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
bot_pattern = "crawl(er)?"