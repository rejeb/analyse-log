/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.udacity.training.analyse.log.buse;

import java.io.File;
import java.util.Arrays;

/**
 *
 * @author r.benrejeb
 */
public class TezJobRunner {

//    public static void main(String[] args) throws Exception {
//        if (args.length != 2) {
//            System.out.printf("Usage: StubDriver <input dir> <output dir>\n" + Arrays.asList(args).toString());
//            System.exit(-1);
//        }
//            DAG dag =new DAG("Log Stats");
//           
//    }
    
        private static String getOutputFolderName(String outputFolder, String[] args, int i) {
        while (new File(outputFolder).exists()) {
            outputFolder = args[1] + "-" + (i++);
        } return outputFolder;
    }
}
