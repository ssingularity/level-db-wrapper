package cn.ist.singularity.test;

import cn.ist.singularity.impl.BaseLevelDBWrapper;
import cn.ist.singularity.wrapper.LevelDBWrapper;
import cn.ist.singularity.wrapper.Operation;
import cn.ist.singularity.wrapper.OperationFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

public class SmallBank {
    public static final String DBPath = "E:\\DB";
    public static final Integer NumOperator = 2;

    private static void db_init(LevelDBWrapper db) {
        List<Operation> operations = new ArrayList<>();
        final Integer numCustomers = 2000;
        final Random r = new Random();
        for (Integer i = 1; !numCustomers.equals(i); ++i) {
            Integer saving = r.nextInt(100000);
            operations.add(OperationFactory.put(i.toString(), saving.toString()));
        }
        db.batch(operations);
    }

    /*
     * generate a sequence of 10 PUT operations and 10 GET operations
     */
    private static List<Operation> doOps() {
        List<Operation> operations = new ArrayList<>();
        final Integer limit = 20;
        for (Integer i = 10; !limit.equals(i); ++i) {
            Integer newSaving = 20201230;
            operations.add(OperationFactory.put(i.toString(), newSaving.toString()));
        }
        for (Integer i = 10; !limit.equals(i); ++i) {
            operations.add(OperationFactory.get(i.toString()));
        }
        /*
        db.batch(operations).forEach((String ret) -> {
            if (! ret.isEmpty()) {
                System.out.println("Operator 1: GET " + ret);
            }
        } );
         */
        return operations;
    }

    /*
     * Generate a sequence of 24 operations,
     * GET interleaves with PUT， 10 for each
     */
    private static List<Operation> doOps2() {
        List<Operation> operations = new ArrayList<>();
        final Integer limit = 20;
        for (Integer i = 8; !limit.equals(i); ++i) {
            operations.add(OperationFactory.get(i.toString()));
            Integer j = i + 4;
            Integer newSaving = 20210101;
            operations.add(OperationFactory.put(j.toString(), newSaving.toString()));
        }
        return operations;
        /*
        db.batch(operations).forEach((String ret) -> {
            if (! ret.isEmpty()) {
                System.out.println("Operator 2: GET " + ret);
            }
        });
         */
    }

    private static class Job implements Runnable {
        private final String name;
        private LevelDBWrapper db;
        private List<Operation> operations;


        public Job(String _name, LevelDBWrapper _db, List<Operation> _ops) {
            name = _name;
            db = _db;
            operations = _ops;
        }

        @Override
        public void run() {
            db.batch(operations).forEach((String ret) -> {
                if (!ret.isEmpty()) {
                    synchronized (SmallBank.class) {
                        System.out.println(name + " : GET " + ret);
                    }
                }
            });
        }
    }

    public static void main(String [] args) {
        // Connect to LevelDB
        LevelDBWrapper db = new BaseLevelDBWrapper(DBPath);

        db_init(db);
        // Create threads to simulate different operator
        ExecutorService executor = Executors.newFixedThreadPool(NumOperator);
        /*
        executor.submit(() -> {
            db.put("version", "1.0");
            db.put("developer", "singularity");
            System.out.println("Thread 1: I feel sleepy... zzzZ");
            Thread.sleep(1500);
            return null;
        });

        executor.submit(() -> {
            Thread.sleep(500);
            String result = db.get("version");
            System.out.println("Thread 2: DB query version GET " + result);
            result = db.get("developer");
            System.out.println("Thread 2: DB query developer GET " + result);
            System.out.println("Thread 2: I want to sleep for a little more... zzzZ");
            Thread.sleep(1000);
            return null;
        });
        */

        // TODO: benchmark

        Job job1 = new Job("Job 1", db, doOps());
        Job job2 = new Job("Job 2", db, doOps2());

        executor.execute(job1);
        executor.execute(job2);

        executor.shutdown();
        /*
        Future<Object> future1 = executor.submit(job1);
        Future<Object> future2 = executor.submit(job2);

        try {
            Object s1 = future1.get();
            Object s2 = future2.get();
        } catch (InterruptedException e) {
            System.out.println("Interrupted");
            e.printStackTrace();
        } catch (ExecutionException e) {
            System.out.println("Execution Error.");
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }

        // executor.shutdown();
         */
    }
}
