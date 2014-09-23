package org.udacity.training.analyse.log.buse.combiner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Tableau2Combiner extends Reducer<Text, Text, Text, Text> {
 private final static String KO="KO";

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException { 
      Map<String,Integer> resultByVerb=new HashMap<>();
      resultByVerb.put(KO, 0);

      for(Text value:values){

          String[] line=value.toString().split(";");  
         
              resultByVerb.put(KO,(resultByVerb.get(KO)+1));
          if(!line[1].equals(KO)){
              if(resultByVerb.containsKey(line[1]))
                  resultByVerb.put(line[1],(resultByVerb.get(line[1])+1));
              else
                  resultByVerb.put(line[1], 1);
          }          
      }
     for(String resultCode:resultByVerb.keySet()){
      context.write(new Text(key),new Text(resultCode+";"+resultByVerb.get(resultCode)) );         
     }
  }
}