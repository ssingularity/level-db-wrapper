package cn.ist.singularity.test;

import cn.ist.singularity.impl.BaseLevelDBWrapper;
import cn.ist.singularity.impl.TwoPLLevelDBWrapper;
import cn.ist.singularity.wrapper.LevelDBWrapper;
import cn.ist.singularity.wrapper.OperationFactory;
import cn.ist.singularity.wrapper.Operations;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

public class BenchMarkTest {
    public static LevelDBWrapper baseLevelDB = new BaseLevelDBWrapper("E:\\db\\base");

    public static LevelDBWrapper twoPLLevelDB = new TwoPLLevelDBWrapper("E:\\db\\2PL");

    public static void main(String[] args) throws Exception {
        baseTest(baseLevelDB);
        baseTest(twoPLLevelDB);

        correctnessTest(baseLevelDB);
        correctnessTest(twoPLLevelDB);

        deadLockTest(baseLevelDB);
        deadLockTest(twoPLLevelDB);

        System.exit(0);
    }

    public static void baseTest(LevelDBWrapper levelDB) throws Exception {
        System.out.println("Base Test for " + levelDB.getClass().getSimpleName());
        Operations operations = Operations.of(OperationFactory.put("test", "1"), OperationFactory.get("test"));
        FutureTask futureTask = new FutureTask(new Transcation(levelDB, operations));
        new Thread(futureTask).start();
        futureTask.get();
        System.out.println("-----------------------------------------------------------");
    }

    public static void correctnessTest(LevelDBWrapper levelDB) throws Exception {
        System.out.println("Correctness Test for " + levelDB.getClass().getSimpleName());
        FutureTask<List<String>> futureTask1 = new FutureTask(new Transcation(levelDB, Operations.of(OperationFactory.put("a", "1"), OperationFactory.get("a"))));
        FutureTask<List<String>> futureTask2 = new FutureTask(new Transcation(levelDB, Operations.of(OperationFactory.put("a", "2"), OperationFactory.get("a"))));
        new Thread(futureTask1).start();
        new Thread(futureTask2).start();
        List<String> transaction1Res = futureTask1.get();
        List<String> transaction2Res = futureTask2.get();
        if (transaction1Res.get(1).equals("get a 1") && transaction2Res.get(1).equals("get a 2")) {
            System.out.println("Correctness Test Pass!");
        }
        else {
            System.out.println("Correctness Fail!");
        }
        futureTask2.get();
        System.out.println("-----------------------------------------------------------");
    }

    public static void deadLockTest(LevelDBWrapper levelDB) throws Exception {
        System.out.println("DeadLock Test for " + levelDB.getClass().getSimpleName());
        levelDB.put("a", "a");
        levelDB.put("b", "b");
        levelDB.put("c", "c");
        FutureTask futureTask1 = new FutureTask(new Transcation(levelDB, Operations.of(OperationFactory.get("a"), OperationFactory.get("b"), OperationFactory.get("c"))));
        FutureTask futureTask2 = new FutureTask(new Transcation(levelDB, Operations.of(OperationFactory.get("c"), OperationFactory.get("b"), OperationFactory.get("a"))));
        new Thread(futureTask1).start();
        new Thread(futureTask2).start();
        try {
            futureTask1.get(2000, TimeUnit.MILLISECONDS);
            futureTask2.get(2000, TimeUnit.MILLISECONDS);
            System.out.println("Dead Lock Test Pass!");
        }
        catch (Exception e) {
            System.out.println("Dead Lock Test Fail!");
        }
        System.out.println("-----------------------------------------------------------");
    }

    private static String resToString(List<String> res) {
        StringBuilder sb = new StringBuilder();
        for (String s: res) {
            sb.append(s);
            sb.append(",");
        }
        String resString = sb.toString();
        return resString.substring(0, resString.length() - 1);
    }

    public static class Transcation implements Callable<List<String>> {
        LevelDBWrapper levelDB;

        Operations operations;

        public Transcation(LevelDBWrapper levelDB, Operations operations) {
            this.levelDB = levelDB;
            this.operations = operations;
        }

        @Override
        public List<String> call() throws Exception {
            System.out.println(Thread.currentThread().getName()+ " start: " + operations.toString());
            List<String> res = levelDB.batch(operations);
            synchronized (BenchMarkTest.class) {
                System.out.println(Thread.currentThread().getName() + " res: " + resToString(res));
            }
            return res;
        }
    }
}
