#include <Rcpp.h>
using namespace Rcpp;

// Identify edits within the session length
// [[Rcpp::export]]
int sessionretriever(NumericVector x, int local_minimum) {
  
  //Instantiate output object
  int output_object = 0;
  
  //For each input object...
  for(int i = 0; i < x.size(); ++i) {
    
    //If the value is below the local minimum...
    if(x[i] <= local_minimum){
      
      //Increment the output object
      output_object += 1;
      
    }
    
  }
  
  //Return
  return output_object;
}