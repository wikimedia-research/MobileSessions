#include <Rcpp.h>
#include <vector>
using namespace Rcpp;

// [[Rcpp::export]]
NumericVector intertime(NumericVector x) {
  
  //Identify size of input object
  int input_size = x.size();
  
  //Instantiate output object
  NumericVector output_vector(input_size-1);

  //Loop over the data
  for(int i = 1; i < input_size;++i){
    
    //For each entry, the output value is [entry] minus [previous entry]
    output_vector[i-1] = (x[i] - x[i-1]);
  }
  
  //Return
  return output_vector;
}