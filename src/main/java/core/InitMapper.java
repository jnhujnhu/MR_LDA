package core;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

import static utils.Settings.*;
import static utils.KeyValueParser.*;

/**
 * Created by Kevin on 11/12/2016.
 */
public class InitMapper extends Mapper<LongWritable, Text, Text, Text> {
    private int[][] WC;
    private Map<String, Integer> VID;

    private int generateUniformSample() {
        return new Random().nextInt(K) + 1;
    }

    @Override
    public void setup(Context context)throws IOException, InterruptedException {
        WC = new int[Vocabulary_No + 1][K + 1];

        //Build WordMap<Term, TermID>
        VID = VariableLoader.Load_WordMap("/users/rocks5/13307130228/corpus/wordmap.ml");
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String doc = value.toString();
        String Words[] = doc.split(" ");
        int[] Z = new int[Max_Word_PerDoc], W = new int[Max_Word_PerDoc];
        //Random assign topic to each word
        for(int i = 0; i < Words.length; i++) {
            Z[i] = generateUniformSample();
            W[i] = VID.get(Words[i]);
//           Counter countPrint1 = context.getCounter("MapperLog :", "Z[i]: " + Integer.toString(Z[i])
//                    + " W[i]: " + Integer.toString(W[i]));
//           countPrint1.increment(1L);
            //Count word assign to topic Z[i]
            WC[W[i]][Z[i]] ++;
        }
        context.write(new Text(encodeKey("Data", key)), new Text(encodeWZ(W, Z)));
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for(int i = 1; i <= Vocabulary_No; i++) {
            context.write(new Text(Integer.toString(i)), new Text(encodeWCi(WC, i)));
        }
    }

}
