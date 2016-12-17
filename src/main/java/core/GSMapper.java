package core;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.Multinomial;

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
        WC = VariableLoader.Load_C("/users/rocks5/13307130228/temp/C/");
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
        //Compute Sigma(WC) on Vocabulary
        int[] Vsum = new int[K + 1];
        for(int k = 1; k <= K; k ++) {
            for (int j = 1; j <= Vocabulary_No; j++) {
                Vsum[k] += WC[j][k];
            }
        }

        for (int i = 0; i < Max_Word_PerDoc; i ++) {
            //Reach the End of this Doc
            if(WZ[0][i] == 0) {
                break;
            }
            DC[WZ[1][i]] --;
            deltaWC[WZ[0][i]][WZ[1][i]] --;
            WC[WZ[0][i]][WZ[1][i]] --;
            Vsum[WZ[1][i]] --;

            //Update WZ[1][i] Using Gibbs Sampling
            double[] probabilities = new double[K + 1];
            double norm = 0;
            for(int j = 1; j <= K; j ++) {
                probabilities[j] = (double) (DC[j]) * (WC[WZ[0][i]][j] + Beta) / (Vsum[j] + Vocabulary_No * Beta);
                norm += probabilities[j];
            }
            double[] normalized_probabilities = new double[K + 1];
            for(int j = 1; j <= K; j ++) {
                normalized_probabilities[j] = probabilities[j] / norm;
            }
            Multinomial multinomial = new Multinomial(normalized_probabilities);
            WZ[1][i] = multinomial.sample();

            DC[WZ[1][i]] ++;
            deltaWC[WZ[0][i]][WZ[1][i]] ++;
            WC[WZ[0][i]][WZ[1][i]] ++;
            Vsum[WZ[1][i]] ++;
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
