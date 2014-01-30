read_data <- function(file){
  
  #Read in the data
  data.df <- read.delim(file = mobile_file,
                        as.is = TRUE,
                        header = TRUE,
                        quote = "",
                        col.names = c("timestamp",
                                      "IP",
                                      "URL_host",
                                      "URL_page",
                                      "URL_params",
                                      "MIME_type",
                                      "referrer",
                                      "UA"))
  
  #Kill MIME types we don't care about
  data.df <- data.df[data.df$MIME_type %in% c("text/html; charset=UTF-8",
                                              "text/html; charset=utf-8",
                                              "text/html; charset=iso-8859-1",
                                              "text/html"),]
  
  #Cut out hits from sites we don't care about.
  data.df <- data.df[grepl(pattern = "((wik(ipedia|isource|tionary|iversity|iquote|ibooks|inews|wikispecies|ivoyage))|(meta|commons|species|incubator)\\.m\\.wikimedia|wikidata|mediawik|boswp|wikpedia|wikipedie|wikiepdia)", x = data.df$URL_host),]
  
  #Cut out pages we don't care about.
  data.df <- data.df[!grepl(pattern = "Special:(BannerRandom|CentralAutoLogin|RecordImpression)", x = data.df$URL_page),]
  
  #Hash IP and UA to come up with a (terrible) proxy for unique users.
  
  #Convert the date column
  
  
  
}