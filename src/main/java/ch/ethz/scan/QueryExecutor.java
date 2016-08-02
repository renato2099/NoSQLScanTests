package ch.ethz.scan;

import ch.ethz.kv.Kv;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Created by renatomarroquin on 2016-08-02.
 */
public class QueryExecutor {
    public static int MAX_QRY_NUM = 22;
    enum ExecutionType {
        SEQUENTIAL, CONCURRENT, PARTIAL_SHARED
    }

    public static void main(String []args) {
        ExecutionType execType = ExecutionType.SEQUENTIAL;
        int nQueries = 10;

        Set<Query> queries = createQueries(nQueries);
        long t0 = System.currentTimeMillis();
        switch(execType) {
            case SEQUENTIAL:
                sequentialQuerying(queries);
                break;
            case CONCURRENT:
                concurrentQuerying();
                break;
            case PARTIAL_SHARED:
                partiallySharing();
                break;
        }
        long tTime = System.currentTimeMillis() - t0;
        System.out.println(String.format("[%s] Total runtime: %d msecs", execType.toString(), tTime));
    }

    private static void partiallySharing() {
    }

    private static void concurrentQuerying() {
    }

    private static void sequentialQuerying(Set<Query> queries) {
        for(Query q : queries) {
            for (Map.Entry<String, Query.Scan> entry : q.scans.entrySet()) {
                Query.Scan qScan = entry.getValue();
                Kv kv = entry.getValue().getKv();
                long t0 = System.currentTimeMillis();
                long nRows = kv.scan(entry.getKey(), qScan.getCol(), qScan.getOp(), qScan.getValue());
                long qTime = System.currentTimeMillis() - t0;
                System.out.println(String.format("[Scan] %s: %d msecs - %d rows", qScan.getTable(), qTime, nRows));
            }
        }
    }

    private static Set<Query> createQueries(int nQueries) {
        Set<Query> qs = new HashSet<>();
        Random r = new Random();
        for (int i = 0; i < nQueries; i++) {
            qs.add(Query.doQuery(r.nextInt(MAX_QRY_NUM)));
        }
        return qs;
    }
}
