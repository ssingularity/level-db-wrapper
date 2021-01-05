package cn.ist.singularity.impl;

import cn.ist.singularity.database.DataBase;
import cn.ist.singularity.impl.dbImpl.LevelDB;
import cn.ist.singularity.impl.dbImpl.MemoryDB;
import cn.ist.singularity.wrapper.LevelDBWrapper;
import cn.ist.singularity.wrapper.Operations;

import java.util.ArrayList;
import java.util.List;

public class BaseLevelDBWrapper implements LevelDBWrapper {
    private DataBase db;

    public BaseLevelDBWrapper() {
        this.db = new MemoryDB();
    }

    public BaseLevelDBWrapper(String filePath) {
        this.db = new LevelDB(filePath);
    }

    @Override
    public void put(String key, String value) {
        this.db.put(key, value);
    }

    @Override
    public void delete(String key) {
        this.db.delete(key);
    }

    @Override
    public String get(String key) {
        return db.get(key);
    }

    @Override
    public List<String> batch(Operations operations) {
        List<String> res = new ArrayList<>();
        operations.forEach(operation -> {
            res.add(operation.onVisit(this));
        });
        return res;
    }
}
