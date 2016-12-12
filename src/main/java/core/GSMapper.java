package core;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static utils.Settings.*;
import static utils.KeyValueParser.*;

/**
 * Created by Kevin on 11/12/2016.
 */
public class GSMapper extends Mapper<LongWritable, Text, Text, Text> {

    private int[][] WC, deltaWC;


    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        WC = VariableLoader.Load_C("/users/rocks5/13307130228/temp/WC/");
        deltaWC = new int[Vocabulary_No + 1][K + 1];
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int[][] WZ = decodeWZ(value.toString());
        //Compute Doc_Topic Vector
        int[] DC = new int[K + 1];
        for(int i = 0; i < Max_Word_PerDoc; i ++) {
            DC[WZ[1][i]] ++;
        }

        for (int i = 0; i < Max_Word_PerDoc; i ++) {
            if(WZ[0][i] == 0) {
                break;
            }
            DC[WZ[1][i]] --;
            deltaWC[WZ[0][i]][WZ[1][i]] --;
            WC[WZ[0][i]][WZ[1][i]] --;

            //TODO: Update WZ[1][i] Using Gibbs Sampling

            DC[WZ[1][i]] ++;
            deltaWC[WZ[0][i]][WZ[1][i]] ++;
            WC[WZ[0][i]][WZ[1][i]] ++;
        }
        context.write(new Text(encodeKey("Data", key)), new Text(encodeWZ(WZ)));
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for(int i = 1; i <= Vocabulary_No; i++) {
            //If is Zero, should Send?
            context.write(new Text(Integer.toString(i)), new Text(encodeWCi(deltaWC, i)));
        }
    }
}
