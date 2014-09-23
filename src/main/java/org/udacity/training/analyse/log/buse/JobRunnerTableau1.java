/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.udacity.training.analyse.log.buse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.udacity.training.analyse.log.buse.combiner.Tableau1Combiner;
import org.udacity.training.analyse.log.buse.mapper.Tableau1Mapper;
import org.udacity.training.analyse.log.buse.reducer.Tableau1Reducer;

/**
 *
 * @author r.benrejeb
 */
public class JobRunnerTableau1 {
    public Boolean run(String inputFile,String outputFolder) throws Exception {

   
     
      
      
       
    /*
     * Instantiate a Job object for your job1's configuration. 
     */
    Job tableau1 = Job.getInstance();
    String separator=";";
    final Configuration conf=tableau1.getConfiguration();
   conf.set("mapreduce.output.textoutputformat.separator", separator);
    /*
     * Specify the jar file that contains your driver, mapper, and reducer.
     * Hadoop will transfer this jar file to nodes in your cluster running 
     * mapper and reducer tasks.
     */
    tableau1.setJarByClass(JobRunnerTableau1.class);
    
    /*
     * Specify an easily-decipherable name for the job1.
     * This job1 name will appear in reports and logs.
     */
    tableau1.setJobName("Log Stats");

    tableau1.setMapperClass(Tableau1Mapper.class);
    tableau1.setReducerClass(Tableau1Reducer.class);
    tableau1.setCombinerClass(Tableau1Combiner.class);
      tableau1.setOutputKeyClass(Text.class);
      tableau1.setOutputValueClass(IntWritable.class);//FloatWritable.class);

      tableau1.setMapOutputKeyClass(Text.class);
      tableau1.setMapOutputValueClass(IntWritable.class);//FloatWritable.class);
      FileInputFormat.addInputPath(tableau1, new Path(inputFile));
      FileOutputFormat.setOutputPath(tableau1, new Path(outputFolder));
    /*
     * Start the MapReduce job1 and wait for it to finish.
     * If it finishes successfully, return 0. If not, return 1.
     */
    boolean success = tableau1.waitForCompletion(true);

    return success;
  }

    
}

