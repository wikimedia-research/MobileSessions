#Load necessary files
source("config.R")
source("functions.R")
source("src/Intertime.cpp")
MobileSessions <- function(){
  
  #Read in data
  data.df <- data_reader()
  
  #Check metadata
  logger()
  
  #Conduct basic analysis and graphing, rework and save
  reworker(analysis())
  
}

#Run
MobileSessions()

#Quit
quit(save = "no")