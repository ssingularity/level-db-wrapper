package cn.ist.singularity.impl;

import cn.ist.singularity.wrapper.LevelDBWrapper;
import cn.ist.singularity.wrapper.Operation;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

public class BaseLevelDBWrapper implements LevelDBWrapper {
    private DB db;

    public BaseLevelDBWrapper(String filePath) {
        Options options = new Options();
        options.createIfMissing(true);
        try {
            this.db = factory.open(new File(filePath), options);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void put(String key, String value) {
        this.db.put(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void delete(String key) {
        this.db.delete(key.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String get(String key) {
        byte[] value = db.get(key.getBytes(StandardCharsets.UTF_8));
        if (value != null) {
            return new String(value);
        } else {
            return null;
        }
    }

    @Override
    public List<String> batch(List<Operation> operations) {
        List<String> res = new ArrayList<>();
        for (Operation operation: operations) {
            switch (operation.type) {
                case Get:
                    res.add(this.get(operation.key));
                    continue;
                case Put:
                    this.put(operation.key, operation.value);
                    continue;
                case Delete:
                    this.delete(operation.key);
                    continue;
                default:
            }
        }
        return res;
    }
}
