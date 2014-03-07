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
  
  #Eliminate bots
  data.df <- data.df[!grepl(x = data.df$UA, ignore.case = TRUE, pattern = bot_pattern),]
  
  #Hash
  data.df <- hasher(data.df)
  
  #Limit to those hashes with >1 article view
  data.df <- data.df[data.df$hash %in% subset(as.data.frame(table(data.df$hash)), Freq > 1)$Var1,]
  
  #Convert timestamps to seconds
  data.df$timestamp <- as.numeric(strptime(x = data.df$timestamp, format = "%Y-%m-%dT%H:%M:%S"))
  
  #Strip unnecessary columns and rename
  data.df <- data.df[,c("timestamp","IP","URL_host","referer")]
  names(data.df) <- c("timestamp","hash","URL","referer")
  
  #Generate interval data
  intervals <- lapper(data.df,lapply_inter,file.path(getwd(),"Data","intertime.RData"))
  
  #Generate from-first data
  fromfirst_data <- lapper(data.df,lapply_first,file.path(getwd(),"Data","fromfirst.RData"))
  
  #Plot
  grapher(intervals,"Data","previous")
  grapher(fromfirst_data,"Data","first")
  
  #Fire off the session length analysis (change the local minimum if reality somehow alters)
  sessionlength(fromfirst_data,430)
}

#Run
MobileSessions()

#Quit
quit(save = "no")