package cn.ist.singularity.test;

import cn.ist.singularity.impl.BaseLevelDBWrapper;
import cn.ist.singularity.impl.TwoPLLevelDBWrapper;
import cn.ist.singularity.impl.WoundWaitingLevelDBWrapper;
import cn.ist.singularity.wrapper.LevelDBWrapper;
import cn.ist.singularity.wrapper.Operation;
import cn.ist.singularity.wrapper.OperationFactory;
import cn.ist.singularity.wrapper.Operations;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

public class DeadLockAndPerformance {
    public static LevelDBWrapper baseLevelDB = new BaseLevelDBWrapper("C:\\db\\base");

    public static LevelDBWrapper twoPLLevelDB = new TwoPLLevelDBWrapper("C:\\db\\2PL");

    public static LevelDBWrapper woundWaitingLevelDB = new WoundWaitingLevelDBWrapper("C:\\db\\wound-waiting");

    public static LevelDBWrapper deadLockBaseLevelDB = new BaseLevelDBWrapper("C:\\db\\dead-lock-base");

    public static LevelDBWrapper deadLockTwoPLLevelDB = new TwoPLLevelDBWrapper("C:\\db\\dead-lock-2PL");

    public static LevelDBWrapper deadLockWoundWaitingLevelDB = new WoundWaitingLevelDBWrapper("C:\\db\\dead-lock-wound-waiting");

    public static LevelDBWrapper baseDB = new BaseLevelDBWrapper();

    public static LevelDBWrapper twoPLDB = new TwoPLLevelDBWrapper();

    public static LevelDBWrapper woundWaitingDB = new WoundWaitingLevelDBWrapper();

    /**
     * To avoid the cache side-affect, please run the performance test one by one
     */
    public static void main(String [] args) {
        deadLockTest(deadLockBaseLevelDB);
//        deadLockTest(deadLockTwoPLLevelDB);
//        deadLockTest(deadLockWoundWaitingLevelDB);
//
//        performanceTest(baseLevelDB);
//        performanceTest(twoPLLevelDB);
//        performanceTest(woundWaitingLevelDB);

//        performanceTest(baseDB);
//        performanceTest(twoPLDB);
//        performanceTest(woundWaitingDB);

        System.exit(0);
    }

    private static Operations genRevertTransaction() {
        List<Operation> ops = new ArrayList<>();
        for (int i = 1; i<=100000; ++i) {
            ops.add(OperationFactory.put(String.valueOf(i), "test"));
        }
        return Operations.of(ops);
    }

    private static Operations genSequentialTransaction() {
        List<Operation> ops = new ArrayList<>();
        for (int i=100000; i>=1; --i) {
            ops.add(OperationFactory.put(String.valueOf(i), "test"));
        }
        return Operations.of(ops);
    }

    private static class Job implements Callable<String> {
        private final String name;
        private LevelDBWrapper db;
        private List<Operations> transactions;

        public Job(String _name, LevelDBWrapper _db, List<Operations> _txns) {
            name = _name;
            db = _db;
            transactions = _txns;
        }

        @Override
        public String call() {
            Instant beginStamp = Instant.now();
            for (Iterator<Operations> it = transactions.iterator(); it.hasNext(); ) {
                db.batch(it.next());
            }
            Instant endStamp = Instant.now();
            return name + " Execution Time: "
                    + Duration.between(beginStamp, endStamp).toMillis()
                    + "ms";
        }
    }

    public static void deadLockTest(LevelDBWrapper db) {
        System.out.println("DeadLock Test for " + db.getClass().getSimpleName());
        baseTest(db, genRevertTransaction(), genSequentialTransaction());
    }

    public static void performanceTest(LevelDBWrapper db) {
        System.out.println("Performance Test for " + db.getClass().getSimpleName());
        baseTest(db, genRevertTransaction(), genRevertTransaction());
    }

    public static void baseTest(LevelDBWrapper db, Operations operations1, Operations operations2) {
        // Create threads to simulate different operator
        ExecutorService executor = Executors.newFixedThreadPool(2);

        Job job1 = new Job("Job 1", db, Arrays.asList(operations1));
        Job job2 = new Job("Job 2", db, Arrays.asList(operations2));

        Instant beginStamp = Instant.now();
        Instant endStamp;
        Future<String> future1 = executor.submit(job1);
        Future<String> future2 = executor.submit(job2);

        try {
            String s1 = future1.get(10000, TimeUnit.MILLISECONDS);
            String s2 = future2.get(10000, TimeUnit.MILLISECONDS);
            endStamp = Instant.now();
            System.out.println(s1);
            System.out.println(s2);
            System.out.println("All jobs complete, " + Duration.between(beginStamp, endStamp).toMillis() + "ms elapsed.");
        } catch (TimeoutException e) {
            System.out.println("10000 milliseconds passed, timeout");
            System.out.println("Probably Deadlock.");
        } catch (Exception ignored) {
        } finally {
            executor.shutdown();
        }
        System.out.println("-----------------------------------------------------------");
    }

}
