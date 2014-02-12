#Load necessary files
source("config.R")
source("functions.R")

MobileSessions <- function(){
  
  #Read in data
  data.df <- data_reader()
  
  #Check metadata
  logger(data.df)
  
  #Conduct basic analysis and graphing
  analysed.df <- analysis.fun(data.df)
  
  #Rework dataset
  
  
  #Save to file
}

#Run
MobileSessions()

#Quit
quit(save = "no")