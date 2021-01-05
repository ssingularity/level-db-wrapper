package cn.ist.singularity.impl;

import cn.ist.singularity.wrapper.Operations;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public class TwoPLLevelDBWrapper extends BaseLevelDBWrapper {
    Map<String, Semaphore> keyLockMap = new ConcurrentHashMap<>();

    public TwoPLLevelDBWrapper() {
        super();
    }

    public TwoPLLevelDBWrapper(String filePath) {
        super(filePath);
    }

    @Override
    public List<String> batch(Operations operations) {
        Set<String> usedKey = new HashSet<>();
        try {
            operations.forEach(operation -> {
                keyLockMap.putIfAbsent(operation.key, new Semaphore(1));
                if (!usedKey.contains(operation.key)) {
                    try {
                        keyLockMap.get(operation.key).acquire();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    usedKey.add(operation.key);
                }
            });
            List<String> res = super.batch(operations);
            return res;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            usedKey.forEach(x -> keyLockMap.get(x).release());
        }
    }
}
