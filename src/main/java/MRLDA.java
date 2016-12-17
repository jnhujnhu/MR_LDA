import core.GSMapper;
import core.InitMapper;
import core.UnifiedReducer;
import org.apache.hadoop.conf.Configuration;
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

import java.io.IOException;

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

}
