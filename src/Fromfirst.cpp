#include <Rcpp.h>
using namespace Rcpp;

// [[Rcpp::export]]
NumericVector fromfirst(NumericVector x) {
  
  //Identify size of input object
  int input_size = x.size();
  
  //Sort
  std::sort(x.begin(),x.end());
  
  //Instantiate output object
  NumericVector output_vector(input_size-1);

  //Loop over the data
  for(int i = 1; i < input_size;++i){
    
    //For each entry, the output value is [entry] minus [first entry]
    output_vector[i-1] = (x[i] - x[0]);
  }
  
  //Return
  return output_vector;
}