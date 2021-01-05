package cn.ist.singularity.database;

/**
 * @Author: ssingualrity
 * @Date: 2021/1/5 21:16
 */
public interface DataBase {
    String get(String key);

    void put(String key, String value);

    void delete(String key);
}
