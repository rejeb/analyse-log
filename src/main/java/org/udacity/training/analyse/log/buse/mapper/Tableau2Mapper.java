package org.udacity.training.analyse.log.buse.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Tableau2Mapper extends Mapper<LongWritable, Text, Text, Text> {
   private static final String errorCode="ER_BUSE_SIU_ERROR";
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    //  System.out.println("Key :"+key+" text :"+value); 
     
         
      String[] val=value.toString().split(";");
     String newLine;
          if(val.length>=11){
           if(errorCode.equals(val[10])){
              newLine=val[7]+";"+val[10];
           context.write(new Text(val[4]+";"+val[3]),new Text(newLine));
           }
              newLine=val[7]+";KO";
      }
    
     
}
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
}
