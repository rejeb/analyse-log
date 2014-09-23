package org.udacity.training.analyse.log.buse.reducer;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Tableau1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {


  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
      int total=0;
      for(IntWritable value:values){
            total+=value.get();       
         
      }
    
           context.write(key,new IntWritable(total) );         
     
  }
  
}