#Load necessary files
source("config.R")
source("functions.R")

MobileSessions <- function(){
  
  
  #If data has already been retrieved...
  if(file.exists(returning_file)){
    
    
    
  } else {
    
    #Otherwise, retrieve the data.
    DataRetrieve()
    
  }
}

#Run
MobileSessions()

#Quit
quit(save = "no")