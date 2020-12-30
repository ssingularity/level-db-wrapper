package cn.ist.singularity.impl;

import cn.ist.singularity.wrapper.Operations;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class TwoPLLevelDBWrapper extends BaseLevelDBWrapper {
    Map<String, ReentrantLock> keyLockMap = new ConcurrentHashMap<>();

    public TwoPLLevelDBWrapper(String filePath) {
        super(filePath);
    }

    @Override
    public List<String> batch(Operations operations) {
        try {
            operations.forEach(operation -> {
                keyLockMap.putIfAbsent(operation.key, new ReentrantLock());
                keyLockMap.get(operation.key).lock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
            List<String> res = super.batch(operations);
            return res;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            operations.forEach(operation -> {
                keyLockMap.get(operation.key).unlock();
            });
        }
    }
}
