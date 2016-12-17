package utils;

import java.util.Random;

/**
 * Created by Kevin on 17/12/2016.
 */
public class Multinomial {
    private double[] Probabilities;

    private int range;

    public Multinomial(double[] probabilities) {
        Probabilities = probabilities;
        range = Probabilities.length + 1;
    }

    public int sample() {
        Random random = new Random();
        double sample = random.nextDouble();
        double start = 0;
        int pos = 1;
        for(int i = 1; i <= range - 1; i ++) {
            if(sample >= start && sample <= start + Probabilities[i]) {
                return pos;
            }
            start += Probabilities[i];
            pos ++;
        }
        return 0;
    }

}
