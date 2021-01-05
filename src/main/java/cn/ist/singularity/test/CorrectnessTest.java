package cn.ist.singularity.test;

import cn.ist.singularity.impl.BaseLevelDBWrapper;
import cn.ist.singularity.impl.TwoPLLevelDBWrapper;
import cn.ist.singularity.impl.WoundWaitingLevelDBWrapper;
import cn.ist.singularity.wrapper.LevelDBWrapper;
import cn.ist.singularity.wrapper.OperationFactory;
import cn.ist.singularity.wrapper.Operations;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

public class CorrectnessTest {
    public static LevelDBWrapper baseLevelDB = new BaseLevelDBWrapper("E:\\db\\base");

    public static LevelDBWrapper twoPLLevelDB = new TwoPLLevelDBWrapper("E:\\db\\2PL");

    public static LevelDBWrapper woundWaitingLevelDbWrapper = new WoundWaitingLevelDBWrapper("E:\\db\\wound-waiting");

    public static void main(String[] args) throws Exception {
        correctnessTest(baseLevelDB);
        correctnessTest(twoPLLevelDB);
        correctnessTest(woundWaitingLevelDbWrapper);

        System.exit(0);
    }

    public static void correctnessTest(LevelDBWrapper levelDB) throws Exception {
        System.out.println("Correctness Test for " + levelDB.getClass().getSimpleName());
        FutureTask<List<String>> futureTask1 = new FutureTask(new Transcation(levelDB, Operations.of(OperationFactory.put("1", "1"), OperationFactory.get("1"))));
        FutureTask<List<String>> futureTask2 = new FutureTask(new Transcation(levelDB, Operations.of(OperationFactory.put("1", "2"), OperationFactory.get("1"))));
        new Thread(futureTask1).start();
        new Thread(futureTask2).start();
        List<String> transaction1Res = futureTask1.get();
        List<String> transaction2Res = futureTask2.get();
        if (transaction1Res.get(1).equals("get 1 1") && transaction2Res.get(1).equals("get 1 2")) {
            System.out.println("Correctness Test Pass!");
        }
        else {
            System.out.println("Correctness Test Fail!");
        }
        futureTask2.get();
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
            synchronized (CorrectnessTest.class) {
                System.out.println(Thread.currentThread().getName() + " res: " + resToString(res));
            }
            return res;
        }
    }
}
