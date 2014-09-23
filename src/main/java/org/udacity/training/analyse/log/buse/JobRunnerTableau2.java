/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.udacity.training.analyse.log.buse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.udacity.training.analyse.log.buse.combiner.Tableau2Combiner;
import org.udacity.training.analyse.log.buse.mapper.Tableau2Mapper;
import org.udacity.training.analyse.log.buse.reducer.Tableau2Reducer;

/**
 *
 * @author r.benrejeb
 */
public class JobRunnerTableau2 {
    public Boolean run(String inputFile,String outputFolder) throws Exception {

        
    /*
     * Instantiate a Job object for your job1's configuration. 
     */
    Job job = Job.getInstance();
    String separator=";";
    final Configuration conf=job.getConfiguration();
      conf.set("mapreduce.output.textoutputformat.separator", separator);

    /*
     * Specify the jar file that contains your driver, mapper, and reducer.
     * Hadoop will transfer this jar file to nodes in your cluster running 
     * mapper and reducer tasks.
     */
    job.setJarByClass(JobRunnerTableau2.class);
    
    /*
     * Specify an easily-decipherable name for the job1.
     * This job1 name will appear in reports and logs.
     */
    job.setJobName("Log Stats");

    job.setMapperClass(Tableau2Mapper.class);
    job.setReducerClass(Tableau2Reducer.class);
    job.setCombinerClass(Tableau2Combiner.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);//FloatWritable.class);

      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);//FloatWritable.class);
      FileInputFormat.addInputPath(job, new Path(inputFile));
      FileOutputFormat.setOutputPath(job, new Path(outputFolder));
    /*
     * Start the MapReduce job1 and wait for it to finish.
     * If it finishes successfully, return 0. If not, return 1.
     */
    boolean success = job.waitForCompletion(true);

    return success;
  }

   
}

