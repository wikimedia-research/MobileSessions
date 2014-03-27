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
#Load necessary files
source("config.R")
source("functions.R")

MobileSessions <- function(){
  
  #Read in data
  data.df <- data_reader()
  
  #Filter and hash
  data.df <- filter(x = data.df)
  
  #Generate interval data
  intervals <- lapper(dataset = data.df, func = lapply_inter, filename = file.path(getwd(),"Data","intertime.RData"))
  
  #Generate from-first data
  fromfirst_data <- lapper(dataset = data.df, func = lapply_first, filename = file.path(getwd(),"Data","fromfirst.RData"), ret_both = TRUE)
  
  #Plot
  grapher(x = intervals[[2]], folder = "Data", datatype = "previous")
  grapher(x = fromfirst_data[[2]], folder = "Data", datatype = "first")
  
  #Fire off the session length analysis (change the local minimum if reality somehow alters)
  sessionlength(x = fromfirst_data[[1]], y = intervals[[1]], local_minimum =  430, datatype = "RequestLogs")
  
  #Check out what ModuleStorage's dataset is doing
  MSdata.df <- rawsql(db_host = "s1-analytics-slave.eqiad.wmnet",
                      db_database = "log",
                      statement = "SELECT event_experimentId AS hash,
                      timestamp
                      FROM ModuleStorage_6978194
                      WHERE left(timestamp,8) BETWEEN '20140116' AND '20140122'
                      AND event_mobileMode IN ('stable','beta','alpha');")
  
  #Filter it - no hashing is needed.
  MSdata.df <- filter(x = MSdata.df, ts_format = "%Y%m%d%H%M%S", prehashed = TRUE)
  
  #Generate interval data and from-first data
  MS_intervals <- lapper(dataset = MSdata.df, func = lapply_inter, filename = file.path(getwd(),"Data","MS_intertime.RData"))
  MS_fromfirst <- lapper(dataset = MSdata.df, func = lapply_first, filename = file.path(getwd(),"Data","MS_fromfirst.RData"))
  
  sessionlength(x = MS_fromfirst[[1]], y = MS_intervals[[1]], local_minimum =  430, datatype = "ModuleStorage")
  
}

#Run
MobileSessions()

#Quit
quit(save = "no")