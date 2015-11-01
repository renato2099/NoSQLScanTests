package ch.ethz.scantest;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


/**
 * Created by marenato on 30.10.15.
 */
public class DataLoader {

    private ExecutorService executors;
    public static int DEFAULT_NTHREADS = 1;
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

    public void load(long nOps, FactoryRunnable.kvStores kvStore, long bSize) {
        Set<Future> futures = new HashSet<>();
        for (int i = 0; i < nThreads; i++) {
            futures.add(executors.submit(FactoryRunnable.getRunnable(kvStore, nOps, bSize)));
        }
        for (Future fut : futures) {
            try {
                fut.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }


    }
}
