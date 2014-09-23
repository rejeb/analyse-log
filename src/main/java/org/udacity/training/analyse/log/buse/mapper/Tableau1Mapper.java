package org.udacity.training.analyse.log.buse.mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Tableau1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
   private final static String ETAT_INCONNU = "ETAT_INCONNU";
    private final static String ETAT_BUSE = "ETAT_BUSE";
    private final static String ETAT_SIU = "ETAT_SIU";
        private final static String ETAT_OK = "OK";
 private final static String TOTAL = "Total";
    private final static List<String> VERB_BUSE=Arrays.asList("CreateParty");
    private final static List<String> VERB_SIU=Arrays.asList("SiuLdapManager.newSiuAccount","UpdateParty", "SiuLdapManager.setAttributes", 
            "SiuLdapManager.addAuthorization", "SiuLdapManager.removeAuthorization", "SiuLdapManager.blockSiuAccount" ,"SiuLdapManager.unblockSiuAccount");
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    //  System.out.println("Key :"+key+" text :"+value); 
     
         
      String[] val=value.toString().split(";");
     String etat;
     IntWritable flag;
      if(isBuseVerb(val[3]))
                etat=ETAT_BUSE;
            else if(isSiuVerb(val[3]))    
                etat=ETAT_SIU;
            else
          etat=ETAT_INCONNU;
          if(val.length>=11){
          
           flag=new IntWritable(1);
          }else{
              
              flag=new IntWritable(0);
             context.write(new Text(ETAT_OK), new IntWritable(1)); 
          }
    context.write(new Text(etat),flag);
    context.write(new Text(TOTAL), new IntWritable(1));
}


    private boolean isBuseVerb(String verb) {
return VERB_BUSE.contains(verb);
    }

    private boolean isSiuVerb(String verb) {
            return VERB_SIU.contains(verb);
    }
}
