package cn.ist.singularity.impl;

import cn.ist.singularity.wrapper.Operation;
import cn.ist.singularity.wrapper.Operations;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Semaphore;


public class Transaction {
    private String id;

    private List<Operation> operations;

    private long timestamp;

    private Status status;

    private WoundWaitingLevelDBWrapper levelDBWrapper;

    private List<Operation> rollBackOperations = new ArrayList<>();

    private List<Semaphore> locks = new ArrayList<>();

    public static Transaction of(Operations operations, WoundWaitingLevelDBWrapper levelDBWrapper) {
        Transaction res = new Transaction();
        res.id = UUID.randomUUID().toString();
        res.operations = operations.intern();
        res.timestamp = System.nanoTime();
        res.status = Status.Running;
        res.levelDBWrapper = levelDBWrapper;
        return res;
    }

    public List<String> start() {
        this.status = Status.Running;
        for (Operation operation : operations) {
            String key = operation.key;
            if (this.id.equals(levelDBWrapper.keyTransactionMap.getOrDefault(key, ""))) {
                continue;
            }
            levelDBWrapper.keyLockMap.putIfAbsent(key, new Semaphore(1));
            Semaphore lock = levelDBWrapper.keyLockMap.get(key);
            while (true) {
                synchronized (this) {
                    if (this.status == Status.Aborted) {
                        return null;
                    }
                    if (lock.tryAcquire()) {
                        this.locks.add(lock);
                        levelDBWrapper.keyTransactionMap.put(key, this.getId());
                        break;
                    }
                    else {
                        String transactionId = levelDBWrapper.keyTransactionMap.get(key);
                        if (transactionId != null) {
                            Transaction transaction = levelDBWrapper.transactionStore.get(transactionId);
                            if (this.isOlderThan(transaction)) {
                                transaction.rollback();
                            }
                        }
                    }
                }
            }
        }
        List<String> res = new ArrayList<>();
        for (Operation operation : operations) {
            synchronized (this) {
                if (this.status == Status.Aborted) {
                    return null;
                }
                rollBackOperations.add(operation.rollBackOperation(levelDBWrapper));
                res.add(operation.onVisit(levelDBWrapper));
            }
        }
        this.finished();
        operations.forEach(operation -> levelDBWrapper.keyTransactionMap.remove(operation.key));
        locks.forEach(Semaphore::release);
        return res;
    }

    public synchronized void rollback() {
        if (this.status == Status.Running) {
            this.aborted();
            System.out.println("Thread " + Thread.currentThread().getName() + " Transaction " + this.getId() + " start rollback");
            rollBackOperations.forEach(operation -> operation.onVisit(levelDBWrapper));
            this.rollBackOperations.clear();
            locks.forEach(Semaphore::release);
        }
    }

    public boolean isOlderThan(Transaction transaction) {
        return this.timestamp < transaction.timestamp;
    }

    public Status getStatus() {
        return this.status;
    }

    synchronized private void finished() {
        if (this.status == Status.Running) {
            this.status = Status.Finished;
        }
    }

    synchronized private void aborted() {
        if (this.status == Status.Running) {
            this.status = Status.Aborted;
        }
    }

    public String getId() {
        return this.id;
    }

    public enum Status {
        Running,
        Aborted,
        Finished;
    }
}
