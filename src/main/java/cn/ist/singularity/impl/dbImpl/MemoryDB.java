package cn.ist.singularity.impl.dbImpl;

import cn.ist.singularity.database.DataBase;

import java.util.HashMap;
import java.util.Map;

public class MemoryDB implements DataBase {
    private Map<String, String> db = new HashMap<>();

    @Override
    public String get(String key) {
        return db.get(key);
    }

    @Override
    public void put(String key, String value) {
        db.put(key, value);
    }

    @Override
    public void delete(String key) {
        db.remove(key);
    }
}
