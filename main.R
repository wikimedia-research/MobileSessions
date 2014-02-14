#Load necessary files
source("config.R")
source("functions.R")

MobileSessions <- function(){
  
  #Read in data
  data.df <- data_reader()
  
  #Check metadata
  logger()
  
  #Conduct basic analysis and graphing
  analysed.df <- analysis(data.df)
  
  #Conduct recursive tree analysis
  tree.df <- tree_analysis(data.df)
  
  #Rework dataset
  reworker(x = list(analysed.df,
                    tree.df)) 
}

#Run
MobileSessions()

#Quit
quit(save = "no")