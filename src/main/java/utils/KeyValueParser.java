package utils;

import org.apache.commons.math3.analysis.function.Max;
import org.apache.hadoop.io.InputBuffer;
import org.apache.hadoop.io.LongWritable;
import static utils.Settings.*;

/**
 * Created by Kevin on 11/12/2016.
 */
public class KeyValueParser {

    public static String encodeKey(String channel, LongWritable d) {
        return channel + " " + d.toString();
    }

    public static String encodeWZ(int[] W, int[] Z) {
        String W_str = "", Z_str = "";
        for(int w : W) {
            W_str += (Integer.toString(w) + " ");
        }
        for(int z : Z) {
            Z_str += (Integer.toString(z) + " ");
        }
        return W_str + "$" + Z_str;
    }

    public static String encodeWZ(int[][] WZ) {
        String W_str = "", Z_str = "";
        for(int w : WZ[0]) {
            W_str += (Integer.toString(w) + " ");
        }
        for(int z : WZ[1]) {
            Z_str += (Integer.toString(z) + " ");
        }
        return W_str + "$" + Z_str;
    }

    public static String encodeWCi(int[][] WC, int voc_no) {
        String WC_str = "";
        for (int j = 1; j <= K; j ++)
            WC_str += (WC[voc_no][j] + " ");
        return WC_str;
    }

    public static String encodeWC(int[][] WC) {
        String C_str = "";
        for (int i = 1; i <= Vocabulary_No; i ++) {
            for(int j = 1; j <= K; j ++) {
                C_str += WC[i][j] + " ";
            }
            C_str += '\n';
        }
        return C_str;
    }

    //TODO
    public static long decodeKey(String key) {
        return 0;
    }

    public static int[][] decodeWZ(String WZ) {
        int[][] WZ_v = new int[2][Max_Word_PerDoc];
        String[] WZ_divide = WZ.split("$");
        String[] W_str = WZ_divide[0].split(" ");
        String[] Z_str = WZ_divide[1].split(" ");
        for(int i = 0; i < Max_Word_PerDoc; i++) {
            WZ_v[0][i] = Integer.parseInt(W_str[i]);
            WZ_v[1][i] = Integer.parseInt(Z_str[i]);
        }
        return WZ_v;
    }

    public static int[] decodeWC(String WC) {
        String[] WC_str = WC.split(" ");
        int[] WC_v = new int[K + 1];
        for(int i = 1; i <= K; i ++) {
            WC_v[i] = Integer.parseInt(WC_str[i - 1]);
        }
        return WC_v;
    }
}
