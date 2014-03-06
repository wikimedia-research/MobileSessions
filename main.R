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
  grapher(intervals,"previous")
  grapher(fromfirst_data,"first")
  
  #Fire off the session length analysis (change the local minimum if reality somehow alters)
  sessionlength(fromfirst_data)
}

#Run
MobileSessions()

#Quit
quit(save = "no")