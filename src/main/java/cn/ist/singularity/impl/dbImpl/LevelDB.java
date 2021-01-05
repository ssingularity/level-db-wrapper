package cn.ist.singularity.impl.dbImpl;

import cn.ist.singularity.database.DataBase;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

public class LevelDB implements DataBase {
    private DB db;

    public LevelDB(String filePath) {
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
    public String get(String key) {
        byte[] value = db.get(key.getBytes(StandardCharsets.UTF_8));
        if (value != null) {
            return new String(value);
        } else {
            return null;
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
}
