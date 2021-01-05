package cn.ist.singularity.impl;

import cn.ist.singularity.wrapper.Operations;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public class WoundWaitingLevelDBWrapper extends BaseLevelDBWrapper{
    Map<String, String> keyTransactionMap = new ConcurrentHashMap<>();

    Map<String, Semaphore> keyLockMap = new ConcurrentHashMap<>();

    Map<String, Transaction> transactionStore = new ConcurrentHashMap<>();

    public WoundWaitingLevelDBWrapper(String filePath) {
        super(filePath);
    }

    @Override
    public List<String> batch(Operations operations) {
        try {
            Transaction transaction = Transaction.of(operations, this);
            transactionStore.put(transaction.getId(), transaction);
            List<String> res = transaction.start();
            while (transaction.getStatus() == Transaction.Status.Aborted) {
                res = transaction.start();
            }
            return res;
        }
        catch (Exception e) {
            e.printStackTrace();
            return new ArrayList<>();
        }
    }
}
