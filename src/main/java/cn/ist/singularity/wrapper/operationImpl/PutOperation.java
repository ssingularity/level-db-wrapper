package cn.ist.singularity.wrapper.operationImpl;

import cn.ist.singularity.wrapper.LevelDBWrapper;
import cn.ist.singularity.wrapper.Operation;

/**
 * @Author: ssingualrity
 * @Date: 2020/12/30 15:59
 */
public class PutOperation extends Operation {
    private String value;

    public PutOperation(String key, String value) {
        super(key);
        this.value = value;
        if (value == null) {
            throw new RuntimeException("值不能为空");
        }
    }

    @Override
    public String onVisit(LevelDBWrapper levelDB) {
        levelDB.put(this.key, this.value);
        return "put " + key + " " + value;
    }

    @Override
    public String toString() {
        return "put " + key + " " + value;
    }
}
