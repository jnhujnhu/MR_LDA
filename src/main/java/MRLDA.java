import core.GSMapper;
import core.InitMapper;
import core.UnifiedReducer;
import core.VariableLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import utils.DocumentInputFormat;
import utils.Multinomial;

import static utils.Settings.*;

import java.io.*;
import java.text.DecimalFormat;
import java.util.Scanner;

/**
 * Created by Kevin on 08/12/2016.
 */
public class MRLDA {

    public static void main(String args[]) throws Exception {
        Configuration config = new Configuration();
        FileSystem fileSystem = FileSystem.get(config);

        if (args[1].equals("restart")) {
            //Clear /temp/C and /temp/WZ
            fileSystem.delete(new Path("/users/rocks5/13307130228/temp/C/"), true);
            fileSystem.delete(new Path("/users/rocks5/13307130228/temp/WZ/"), true);

            //INIT JOB
            Job job_init = Job.getInstance(config, "MR_LDA_Init");
            job_init.setJarByClass(MRLDA.class);
            job_init.setMapperClass(InitMapper.class);
            job_init.setMapOutputKeyClass(Text.class);
            job_init.setMapOutputValueClass(Text.class);
            job_init.setReducerClass(UnifiedReducer.class);
            MultipleInputs.addInputPath(job_init, new Path("/users/rocks5/13307130228/corpus/corpus.cp")
                    , DocumentInputFormat.class);
            MultipleOutputs.addNamedOutput(job_init, "WC", TextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job_init, "WZ", TextOutputFormat.class, Text.class, Text.class);
            FileOutputFormat.setOutputPath(job_init, new Path("/users/rocks5/13307130228/result"));
            fileSystem.delete(new Path("/users/rocks5/13307130228/result"), true);
            job_init.waitForCompletion(true);
        }
        else if(args[1].equals("output")) {

            fileSystem.delete(new Path("/users/rocks5/13307130228/model/test.phi"), true);
            fileSystem.delete(new Path("/users/rocks5/13307130228/model/test.theta"), true);

            fileSystem.create(new Path("/users/rocks5/13307130228/model/test.phi"));
            fileSystem.create(new Path("/users/rocks5/13307130228/model/test.theta"));

            String WZ_Path = "/users/rocks5/13307130228/temp/WZ/";
            int[][] C = VariableLoader.Load_C("/users/rocks5/13307130228/temp/C/");
            int[] Ksum = new int[Vocabulary_No + 1];
            DecimalFormat df = new DecimalFormat("0.000000");
            for (int j = 1; j <= K; j++) {
                for (int k = 1; k <= Vocabulary_No; k++) {
                    Ksum[j] += C[k][j];
                }
            }
            String outs = "";

            for (int j = 1; j <= K; j++) {
                double[] phi = new double[Vocabulary_No + 1];
                for (int k = 1; k <= Vocabulary_No; k++) {
                    phi[k] = (double) (C[k][j] + Beta) / (Ksum[j] + Vocabulary_No * Beta);
                    outs += df.format(phi[k]) + " ";
                }
                outs += "\n";
            }
            FSDataOutputStream output = fileSystem.create(new Path("/users/rocks5/13307130228/model/test.phi"), true);
            output.write(outs.getBytes());
            output.close();
            String theta = "";
            FileStatus[] status_list = fileSystem.listStatus(new Path(WZ_Path));
            if (status_list != null) {
                for (FileStatus status : status_list) {
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem
                            .open(status.getPath())));
                    Scanner scanner = new Scanner(bufferedReader);
                    while (scanner.hasNextLine()) {
                        String line = scanner.nextLine();
                        int[] Z = new int[Max_Word_PerDoc];
                        int doc_no = Integer.parseInt(line.split("\t")[0]);
                        String[] temp_W = line.split("\t")[2].split(" ");
                        for (int i = 0; i < temp_W.length; i++) {
                            Z[i] = Integer.parseInt(temp_W[i]);
                        }
                        //Compute Doc_Topic Vector
                        int[] DC = new int[K + 1];
                        for (int i = 0; i < Max_Word_PerDoc; i++) {
                            DC[Z[i]]++;
                        }
                        int Dsum = 0;
                        for (int i = 1; i <= K; i++) {
                            Dsum += DC[i];
                        }

                        for (int i = 1; i <= K; i++) {
                            double temp_theta = 0;
                            temp_theta = (double) (DC[i] + Alpha) / (Dsum + K * Alpha);
                            theta += df.format(temp_theta) + " ";
                        }
                        theta += "\n";

                    }
                }
                FSDataOutputStream out = fileSystem.create(new Path("/users/rocks5/13307130228/model/test.theta"), true);
                out.write(theta.getBytes());
                out.close();
            }
            System.exit(0);
        }
        int IterationCount = Iteration_No;
        if (Iteration_No == -1) {
            IterationCount = Integer.parseInt(args[0]);
        }
        for (int i = 0; i < IterationCount; i++) {
            //Gibbs Sampling ITERATION JOB
            Job job_iteration = Job.getInstance(config, "MR_LDA_Iteration");
            job_iteration.setJarByClass(MRLDA.class);
            job_iteration.setMapperClass(GSMapper.class);
            job_iteration.setMapOutputKeyClass(Text.class);
            job_iteration.setMapOutputValueClass(Text.class);
            job_iteration.setReducerClass(UnifiedReducer.class);
            MultipleInputs.addInputPath(job_iteration, new Path("/users/rocks5/13307130228/temp/WZ/")
                    , DocumentInputFormat.class);
            MultipleOutputs.addNamedOutput(job_iteration, "WC", TextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job_iteration, "WZ", TextOutputFormat.class, Text.class, Text.class);
            FileOutputFormat.setOutputPath(job_iteration, new Path("/users/rocks5/13307130228/result"));
            fileSystem.delete(new Path("/users/rocks5/13307130228/result"), true);
            if (i != IterationCount - 1)
                job_iteration.waitForCompletion(true);
            else
                System.exit(job_iteration.waitForCompletion(true) ? 0 : 1);
        }

    }

//    public static void main(String args[]) throws Exception {
//        BufferedReader bf = new BufferedReader(new InputStreamReader(new FileInputStream("/Users/Kevin/Desktop/Courseware/Distribute_System/corpus.cp")));
//        Scanner scanner = new Scanner(bf);
//        int i = 1;
//        int maxlen = -1;
//        while(scanner.hasNextLine()) {
//            String line = scanner.nextLine();
//            int len = line.split(" ").length;
//            if(len > maxlen)
//                maxlen = len;
//            i ++;
//        }
//        System.out.println(maxlen);
//        bf.close();
//
//    }
}
