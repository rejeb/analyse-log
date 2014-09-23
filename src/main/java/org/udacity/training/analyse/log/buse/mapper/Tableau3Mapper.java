package org.udacity.training.analyse.log.buse.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Tableau3Mapper extends MapReduceBase
    implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, Text> {

//  public String replaceAll(String input){
//   String[] separators=".,!,:,?,\",<,>,(,),[,],;,$,=,-,/".split(",");
//   
//   for(String separator:separators)
//   input=input.replace(separator, " ");
//   
//   return input.replace(",", " ");
//  }
//  
//  public String formatLine(String uid,String prefix,String[] line){
//       
//      String formattedLine = line[3]+"\t"+ "B"+"\t"+ line[0]+"\t"+ line[1]+"\t"+  line[2]+"\t"+  line[5]+"\t"+  line[6]+"\t"+  line[7]+"\t"+  line[8]+"\t"+  line[9];
//      
//      
//      return formattedLine;
//  }
//   public String formatUserInfo(String uid,String prefix,String[] line){
//      String formattedLine=prefix;
//      for(int i=1;i<line.length;i++)
//          formattedLine+="\t"+line[i];
//      
//      return formattedLine;
//  }  

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

         
      String[] val=value.toString().split(";");
      if(val.length>=10){
     String newLine;
          if(val[8].equals("0"))
              newLine=val[8]+";"+val[10];
          else
              newLine=val[8]+";OK";
    output.collect(new Text(val[5]+";"+val[4]),new Text(newLine));
      }    
    }
}
