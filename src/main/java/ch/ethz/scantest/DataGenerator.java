package ch.ethz.scantest;

import org.apache.hadoop.hbase.util.Strings;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by renatomarroquin on 2015-11-01.
 */
public class DataGenerator {
    private static final char[] symbols;
    private static final String fiveStr = "12345";
    private static final String tenStr = "1234567890";

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

    public String genFixedText(int size) {
        int nTens = size / 10;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < nTens; i ++)
            sb.append(tenStr);
        if (nTens*10 != size)  {
            int nFives = (size-nTens*10)/5;
            for (int i = 0; i < nFives; i ++)
                sb.append(fiveStr);
        }
        return sb.toString();
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

    public int genInt(int bound) {
        return random.nextInt(bound);
    }

    public int genInt() {
        return random.nextInt();
    }

    public String getCountry() {
        return COUNTRIES[random.nextInt(COUNTRIES.length)];
    }
}
