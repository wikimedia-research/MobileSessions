#include <Rcpp.h>
using namespace Rcpp;

// Identify edits within the session length
// [[Rcpp::export]]
int sessionretriever(NumericVector x, int local_minimum) {
  
  //Instantiate output object. It has the value of 1 to handle R's indexing.
  int output_object = 1;
  
  //For each input object...
  for(int i = 0; i < x.size(); ++i) {
    
    //If the value is below the local minimum...
    if(x[i] <= local_minimum){
      
      //Increment the output object
      output_object += 1;
      
    }
    else {
      
      //When you encounter the first inter-time period above the local minimum, just return the output object.
      return output_object;
      
    }
    
  }
  
  //Return
  return output_object;
}