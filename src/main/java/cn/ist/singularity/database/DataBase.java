package cn.ist.singularity.database;

public interface DataBase {
    String get(String key);

    void put(String key, String value);

    void delete(String key);
}
