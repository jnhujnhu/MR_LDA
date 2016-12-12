package core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


import java.io.IOException;

import static utils.KeyValueParser.*;
import static utils.Settings.*;

/**
 * Created by Kevin on 11/12/2016.
 */
public class UnifiedReducer extends Reducer<Text, Text, Text, Text> {

    private MultipleOutputs outputs;
    private int[][] C;
    private String C_Buf_Path;
    private String C_Path;
    private String WZ_Path;
    private String WZ_Buf_Path;
    private FileSystem fileSystem;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        outputs = new MultipleOutputs(context);
        fileSystem = FileSystem.get(new Configuration());

        C = new int[Vocabulary_No + 1][K + 1];

        C_Buf_Path = "/users/rocks5/13307130228/temp/C_Buf/";
        C_Path = "/users/rocks5/13307130228/temp/C/";

        C = VariableLoader.Load_C(C_Path);

        WZ_Path = "/users/rocks5/13307130228/temp/WZ/";
        WZ_Buf_Path = "/users/rocks5/13307130228/temp/WZ_Buf/";

    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int[] WC_i = new int[K + 1];
        if(key.toString().startsWith("Data")) {
            outputs.write(new Text(key.toString().substring(5)), values.iterator().next(), WZ_Buf_Path + "part");
        }
        else {
            for(Text text: values) {
                for(int pos = 1;pos <= K; pos++) {
                    WC_i[pos] += decodeWC(text.toString())[pos];
                }
            }
            for(int pos = 1;pos <= K; pos++) {
                C[Integer.parseInt(key.toString())][pos] += WC_i[pos];
            }
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        outputs.write(NullWritable.get(), new Text(encodeWC(C)), C_Buf_Path + "part");

        fileSystem.delete(new Path(C_Path), true);
        fileSystem.rename(new Path(C_Buf_Path), new Path(C_Path));

        if(fileSystem.exists(new Path(WZ_Path))) {
            fileSystem.delete(new Path(WZ_Path), true);
        }
        fileSystem.rename(new Path(WZ_Buf_Path), new Path(WZ_Path));

        outputs.close();
        fileSystem.close();
        super.cleanup(context);
    }
}
