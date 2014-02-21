#Load necessary files
source("config.R")
source("functions.R")
sourceCpp("src/Intertime.cpp")
sourceCpp("src/Totaltime.cpp")

MobileSessions <- function(){
  
  #Read in data
  data.df <- data_reader()
  
  #Check metadata
  logger()
  
  #Conduct basic analysis and graphing, rework and save
  analysis()

}

#Run
MobileSessions()

#Quit
quit(save = "no")