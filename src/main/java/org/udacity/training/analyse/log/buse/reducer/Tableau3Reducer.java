package org.udacity.training.analyse.log.buse.reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Tableau3Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    private static final String OK="OK";
 

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      Map<String,Integer> resultByVerb=new HashMap<>();
      int total=0;
      while(values.hasNext()){
          Text value=values.next();
          total++;
          String valueString=value.toString();
          String[] line=valueString.split(";");  
          
              if(resultByVerb.containsKey(line[1]))
                  resultByVerb.put(line[1],(resultByVerb.get(line[1])+1));
              else
                  resultByVerb.put(line[1], 1);
         
      }
     for(String resultCode:resultByVerb.keySet()){
      output.collect(new Text(key),new Text(resultCode+";"+resultByVerb.get(resultCode)+";"+total) );         
     }
    }

   
  
}