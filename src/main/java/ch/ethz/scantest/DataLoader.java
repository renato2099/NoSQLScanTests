package ch.ethz.scantest;

import org.mortbay.log.Log;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static ch.ethz.scantest.kv.Kv.*;


/**
 * Created by marenato on 30.10.15.
 */
public class DataLoader {

    private ExecutorService executors;
    public static int DEFAULT_NTHREADS = 4;
    private int nThreads;

    public DataLoader() {
        initialize(DEFAULT_NTHREADS);
    }

    public DataLoader(int nthreads) {
        initialize(nthreads);
    }

    private void initialize(int nt) {
        nThreads = nt;
        executors = Executors.newFixedThreadPool(nt);
    }

    public void load(long nOps, kvStores kvStore, long bSize) {
        Set<Future> futures = new HashSet<>();
        long extras = nOps % nThreads != 0?nOps % nThreads:0;

        long tOps = nOps/nThreads;
        for (int i = 0; i < nThreads-1; i++) {
            long rStart = (i*tOps)+1;
            futures.add(executors.submit(FactoryRunnable.getRunnable(kvStore, tOps, bSize, rStart)));
        }
        long rStart = ((nThreads-1)*tOps)+1;
        futures.add(executors.submit(FactoryRunnable.getRunnable(kvStore, tOps + extras, bSize, rStart)));

        for (Future fut : futures) {
            try {
                fut.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        Log.info(String.format("[Load %s] All threads completed", kvStore.toString()));
    }
}
