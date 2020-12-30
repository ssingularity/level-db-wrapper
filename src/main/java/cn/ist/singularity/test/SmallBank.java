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
        for (Integer i = 0; i != numCustomers; ++i) {
            Integer saving = r.nextInt(100000);
            operations.add(OperationFactory.put(i.toString(), saving.toString()));
        }
        db.batch(operations);
    }

    private static void doOps(LevelDBWrapper db) {
        List<Operation> operations = new ArrayList<>();
        final Integer limit = 20;
        for (Integer i = 10; i != limit; ++i) {
            Integer newSaving = 20201230;
            operations.add(OperationFactory.put(i.toString(), newSaving.toString()));
        }
        for (Integer i = 10; i != limit; ++i) {
            operations.add(OperationFactory.get(i.toString()));
        }
        db.batch(operations).forEach((String ret) -> {
            if (! ret.isEmpty()) {
                System.out.println("Operator 1: GET " + ret);
            }
        } );
    }

    private static void doOps2(LevelDBWrapper db) {
        List<Operation> operations = new ArrayList<>();
        final Integer limit = 20;
        for (Integer i = 8; i != limit; ++i) {
            operations.add(OperationFactory.get(i.toString()));
            Integer j = i + 4;
            Integer newSaving = 20210101;
            operations.add(OperationFactory.put(j.toString(), newSaving.toString()));
        }
        db.batch(operations).forEach((String ret) -> {
            if (! ret.isEmpty()) {
                System.out.println("Operator 2: GET " + ret);
            }
        });
    }


    public static void main(String [] args) {
        // Connect to LevelDB
        LevelDBWrapper db = new BaseLevelDBWrapper(DBPath);

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

        Future<Object> future1 = executor.submit(() -> {
            doOps(db);
            return null;
        } );
        Future<Object> future2 =  executor.submit(() -> {
            doOps2(db);
            return null;
        });

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
    }
}
