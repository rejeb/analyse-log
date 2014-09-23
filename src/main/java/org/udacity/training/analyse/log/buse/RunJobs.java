package org.udacity.training.analyse.log.buse;

import java.io.File;
import java.util.Arrays;

/**
 *
 * @author r.benrejeb
 */
public class RunJobs {

    public static void main(String[] args) throws Exception {
         if (args.length != 2) {
      System.out.printf("Usage: StubDriver <input dir> <output dir>\n"+Arrays.asList(args).toString());
      System.exit(-1);
    }
  
         int i=0;
		
         String outputFolder=getOutputFolderName(args[1], args, i);
         args[1]=outputFolder;
         JobRunnerTableau3 jobRunnerTableau3 = new JobRunnerTableau3();
        int success = jobRunnerTableau3.run(args);
//        if (success) {
//            JobRunnerTableau2 jobRunnerTableau2 = new JobRunnerTableau2();
//            success = jobRunnerTableau2.run("D:/hadoop/workspaces/docs/input/log_buse_all.log",outputFolder+"/tableau2");
//        } else {
//            System.out.println("Error while executing Job 1");
//        }
//        if (success) {
//            JobRunnerTableau3 jobRunnerTableau3 = new JobRunnerTableau3();
//            success = jobRunnerTableau3.run("D:/hadoop/workspaces/docs/input/log_buse_all.log",outputFolder+"/tableau3");
//        } else {
//            System.out.println("Error while executing Job 2");
//        }
//        if (!success) {
//            System.out.println("Error while executing Job 3");
//        }

        System.exit(success);
    }
    
      private static String getOutputFolderName(String outputFolder, String[] args, int i) {
        while (new File(outputFolder).exists()) {
            outputFolder = args[1] + "-" + (i++);
        } return outputFolder;
    }
}
