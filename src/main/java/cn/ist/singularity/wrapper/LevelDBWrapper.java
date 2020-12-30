package cn.ist.singularity.wrapper;

import java.util.List;

public interface LevelDBWrapper {
    void put(String key, String value);

    void delete(String key);

    void get(String key);

    void batch(List<Operation> operations);
}
