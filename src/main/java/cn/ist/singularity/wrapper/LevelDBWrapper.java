package cn.ist.singularity.wrapper;

import java.util.List;

public interface LevelDBWrapper {
    void put(String key, String value);

    void delete(String key);

    String get(String key);

    /**
     * 针对操作列表实现批操作，可以将operations视为一个事务
     *
     * @param operations 操作列表
     * @return 所有操作的结果列表，其顺序与operations顺序相符
     */
    List<String> batch(Operations operations);
}
