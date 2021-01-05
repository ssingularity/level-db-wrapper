package cn.ist.singularity.wrapper.operationImpl;

import cn.ist.singularity.wrapper.LevelDBWrapper;
import cn.ist.singularity.wrapper.Operation;
import cn.ist.singularity.wrapper.OperationFactory;

/**
 * @Author: ssingualrity
 * @Date: 2020/12/30 15:59
 */
public class PutOperation extends Operation {
    private String value;

    public PutOperation(String key, String value) {
        super(key);
        this.value = value;
    }

    @Override
    public String onVisit(LevelDBWrapper levelDB) {
        levelDB.put(this.key, this.value);
        return "put " + key + " " + value;
    }

    @Override
    public Operation rollBackOperation(LevelDBWrapper levelDB) {
        String oldValue = levelDB.get(this.key);
        return OperationFactory.put(this.key, oldValue);
    }

    @Override
    public String toString() {
        return "put " + key + " " + value;
    }
}
