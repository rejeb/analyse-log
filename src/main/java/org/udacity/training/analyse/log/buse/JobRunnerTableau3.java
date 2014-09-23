/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.udacity.training.analyse.log.buse;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;
import org.udacity.training.analyse.log.buse.combiner.Tableau3Combiner;
import org.udacity.training.analyse.log.buse.mapper.Tableau3Mapper;
import org.udacity.training.analyse.log.buse.reducer.Tableau3Reducer;

/**
 *
 * @author r.benrejeb
 */
public class JobRunnerTableau3 extends Configured implements Tool{

    public int run(String inputFile, String outputFolder) throws Exception {

        /*
         * Instantiate a Job object for your job1's configuration. 
         */
        JobConf conf = new JobConf(JobRunnerTableau3.class);
        String separator = ";";
           conf.set("mapreduce.output.textoutputformat.separator", separator);
           
       
        

        /*
         * Specify an easily-decipherable name for the job1.
         * This job1 name will appear in reports and logs.
         */
        conf.setJobName("Log Stats");

        conf.setMapperClass(Tableau3Mapper.class);
        conf.setReducerClass(Tableau3Reducer.class);
        conf.setCombinerClass(Tableau3Combiner.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);//FloatWritable.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);//FloatWritable.class);
        FileInputFormat.addInputPath(conf, new Path(inputFile));
        FileOutputFormat.setOutputPath(conf, new Path(outputFolder));
        /*
         * Start the MapReduce job1 and wait for it to finish.
         * If it finishes successfully, return 0. If not, return 1.
         */
       RunningJob rj= JobClient.runJob(conf);
       rj.waitForCompletion();
       return rj.isSuccessful()?0:1;
    }

    @Override
    public int run(String[] args) throws Exception {
        
           return run(args[0],args[1]);

    }

}
