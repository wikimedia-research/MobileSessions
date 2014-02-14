#include <Rcpp.h>
using namespace Rcpp;

// [[Rcpp::export]]
int totaltime(NumericVector x, int local_minimum) {
  
  //Instantiate output object
  int output_object = 0;
  int output_size = 0;
  
  //For each input object...
  for(int i = 0; i < x.size(); ++i) {
    
    //If the value is below the local minimum...
    if(x[i] <= local_minimum){
      
      //Add it to the output object
      output_object += x[i];
      
      //Increment the size counter
      output_size += 1;
      
    }
    
  }
  
  //Add a mean value to the output object to factor in the time period for the 'last' read
  output_object += (output_object/output_size);
  
  //Return
  return output_object;
}
