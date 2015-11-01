package ch.ethz.scantest;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by renatomarroquin on 2015-11-01.
 */
public class DataGenerator {
    private static final char[] symbols;

    static {
        StringBuilder tmp = new StringBuilder();
        for (char ch = '0'; ch <= '9'; ++ch)
            tmp.append(ch);
        for (char ch = 'a'; ch <= 'z'; ++ch)
            tmp.append(ch);
        symbols = tmp.toString().toCharArray();
    }

    private ThreadLocalRandom random;

    public static String [] COUNTRIES = {"aaaaa", "bbbbb", "ccccc", "eeeee", "fffff"};

    public DataGenerator() {
        this.random = ThreadLocalRandom.current();
    }

    public String genText(int size) {
        char[] buf = new char[size];
        for (int i = 0; i < size; i++) {
            buf[i] = symbols[random.nextInt(symbols.length)];
        }
        return new String(buf);
    }

    public double genDouble() {
        return random.nextDouble();
    }

    public int genInt() {
        return random.nextInt();
    }

    public String getCountry() {
        return COUNTRIES[random.nextInt(COUNTRIES.length)];
    }
}
