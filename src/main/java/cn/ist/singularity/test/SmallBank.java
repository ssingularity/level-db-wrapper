package cn.ist.singularity.test;

import cn.ist.singularity.impl.TwoPLLevelDBWrapper;
import cn.ist.singularity.wrapper.LevelDBWrapper;
import cn.ist.singularity.wrapper.Operation;
import cn.ist.singularity.wrapper.OperationFactory;
import cn.ist.singularity.wrapper.Operations;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

public class SmallBank {
    public static final String DBPath = "E:\\DB";
    public static final Integer NumOperator = 2;

    /*
     * Insert 2000 records into database.
     * Set each account's saving is random
     */
    private static void db_init(LevelDBWrapper db) {
        List<Operation> operations = new ArrayList<>();
        final Integer numCustomers = 2000;
        final Random r = new Random();
        for (Integer i = 1; !numCustomers.equals(i); ++i) {
            Integer saving = r.nextInt(100000);
            operations.add(OperationFactory.put(i.toString(), saving.toString()));
        }
        db.batch(Operations.of(operations));
    }

    /*
     * generate a sequence of 10 PUT operations and 10 GET operations
     */
    private static Operations doOps() {
        List<Operation> operations = new ArrayList<>();
        final Integer limit = 20;
        for (Integer i = 10; !limit.equals(i); ++i) {
            Integer newSaving = 20201230;
            operations.add(OperationFactory.put(i.toString(), newSaving.toString()));
        }
        for (Integer i = 10; !limit.equals(i); ++i) {
            operations.add(OperationFactory.get(i.toString()));
        }
        return Operations.of(operations);
    }

    /*
     * Generate a sequence of 24 operations,
     * GET interleaves with PUT， 10 for each
     */
    private static Operations doOps2() {
        List<Operation> operations = new ArrayList<>();
        final Integer limit = 20;
        for (Integer i = 8; !limit.equals(i); ++i) {
            operations.add(OperationFactory.get(i.toString()));
            Integer j = i + 4;
            Integer newSaving = 20210101;
            operations.add(OperationFactory.put(j.toString(), newSaving.toString()));
        }
        return Operations.of(operations);
    }

    /*
     * 1000 PUT[1-1000] + 1000 GET[1-1000]
     */
    private static Operations genOp1() {
        List<Operation> ops = new ArrayList<>();
        final Integer limit = 1000;
        Integer setting1 = 18960231;
        for (Integer i = 1; !limit.equals(i); ++i) {
            ops.add(OperationFactory.put(i.toString(), setting1.toString()));
            ++setting1;
        }
        for (Integer i = 1; !limit.equals(i); ++i) {
            ops.add(OperationFactory.get(i.toString()));
        }
        return Operations.of(ops);
    }

    /*
     * 1000 GET[100-1,101-200,300-201,...901-999]
     * 1000 PUT[1000-1999]
     */
    private static Operations genOp2() {
        List<Operation> ops = new ArrayList<>();
        final Integer limit = 1000;
        Integer setting2 = 20201230;
        for (Integer i = 0; !i.equals(10); ++i) {
            if (i % 2 == 0) {
                for (Integer j = (i + 1) * 100; !j.equals(i * 100); --j) {
                    ops.add(OperationFactory.get(j.toString()));
                }
            }
            if (i % 2 == 1) {
                for (Integer j = i * 100 + 1; !j.equals((i + 1) * 100); ++j) {
                    ops.add(OperationFactory.get(j.toString()));
                }
            }
        }
        for (Integer i = 1000; !i.equals(2000); ++i) {
            ops.add(OperationFactory.put(i.toString(), setting2.toString()));
        }
        return Operations.of(ops);
    }

    /*
     * generate a list of transactions - 1
     */
    private static List<Operations> genBaseTransactions() {
        List<Operations> txns = new ArrayList<>();
        txns.add(Operations.of(
                OperationFactory.get("1"),
                OperationFactory.put("1", "18960231")
        ));
        txns.add(Operations.of(
                OperationFactory.get("2"),
                OperationFactory.get("3"),
                OperationFactory.put("2", "18961313"),
                OperationFactory.put("3", "18961414")
        ));
        txns.add(Operations.of(
                OperationFactory.get("5"),
                OperationFactory.put("5", "18960303"),
                OperationFactory.get("5")
        ));
        txns.add(Operations.of(
                OperationFactory.put("10", "18961010"),
                OperationFactory.put("11", "18961111"),
                OperationFactory.put("12", "18961212")
        ));
        return txns;
    }

    /*
     * race with 1, but no dead lock
     */
    private static List<Operations> genRaceConditionTransactions() {
        List<Operations> txns = new ArrayList<>();
        txns.add(Operations.of(
                OperationFactory.get("6"),
                OperationFactory.put("6", "20201231")
        ));
        txns.add(Operations.of(
                OperationFactory.get("4"),
                OperationFactory.get("2"),
                OperationFactory.put("2", "20200909"),
                OperationFactory.put("4", "20201010")
        ));
        txns.add(Operations.of(
                OperationFactory.get("7"),
                OperationFactory.put("7", "20200501"),
                OperationFactory.get("7")
        ));
        txns.add(Operations.of(
                OperationFactory.put("14", "20200601"),
                OperationFactory.put("13", "20200701"),
                OperationFactory.put("12", "20200801")
        ));
        return txns;
    }

    /*
     * It is possible to deadlock when running with 1 concurrently
     */
    private static List<Operations> genDeadLockTransactions() {
        List<Operations> txns = new ArrayList<>();
        txns.add(Operations.of(
                OperationFactory.get("6"),
                OperationFactory.put("6", "30121231")
        ));
        txns.add(Operations.of(
                OperationFactory.get("3"),
                OperationFactory.get("2"),
                OperationFactory.put("2", "30120909"),
                OperationFactory.put("3", "30121010")
        ));
        txns.add(Operations.of(
                OperationFactory.get("7"),
                OperationFactory.put("7", "30120501"),
                OperationFactory.get("7")
        ));
        txns.add(Operations.of(
                OperationFactory.put("13", "30120601"),
                OperationFactory.put("12", "30120701"),
                OperationFactory.put("12", "30120801")
        ));
        return txns;
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
            Integer counter = 1;
            for (Iterator<Operations> it = transactions.iterator(); it.hasNext(); ) {
                db.batch(it.next()).forEach((String ret) -> {
                    synchronized (SmallBank.class) {
                        System.out.println(name + ": " + ret);
                    }
                });
                synchronized (SmallBank.class) {
                    System.out.println(name + " Transaction " + counter + " Done\n"
                            + "---------------------------------------------");
                }
                ++counter;
            }
            Instant endStamp = Instant.now();
            return name + " Execution Time: "
                    + Duration.between(beginStamp, endStamp).toMillis()
                    + "ms";
        }
    }

    public static void main(String [] args) {
        // Connect to LevelDB
        // LevelDBWrapper db = new BaseLevelDBWrapper(DBPath);
        LevelDBWrapper db = new TwoPLLevelDBWrapper(DBPath);
//        LevelDBWrapper db = new WoundWaitingLevelDBWrapper(DBPath);

        // Initial database
        db_init(db);
        // Create threads to simulate different operator
        ExecutorService executor = Executors.newFixedThreadPool(NumOperator);

        // TODO: benchmark

        Job job1 = new Job("Job 1", db, genBaseTransactions());
        Job job2 = new Job("Job 2", db, genRaceConditionTransactions());

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
            // e.printStackTrace();
        } catch (InterruptedException e) {
            System.out.println("Interrupted");
            e.printStackTrace();
        } catch (ExecutionException e) {
            System.out.println("Execution Error.");
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }

        System.exit(0);
    }
}
