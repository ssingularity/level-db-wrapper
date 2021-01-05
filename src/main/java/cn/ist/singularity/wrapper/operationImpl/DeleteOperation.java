package cn.ist.singularity.wrapper.operationImpl;

import cn.ist.singularity.wrapper.LevelDBWrapper;
import cn.ist.singularity.wrapper.Operation;
import cn.ist.singularity.wrapper.OperationFactory;

public class DeleteOperation extends Operation {
    public DeleteOperation(String key) {
        super(key);
    }

    @Override
    public String onVisit(LevelDBWrapper levelDB) {
        levelDB.delete(this.key);
        return "del " + key;
    }

    @Override
    public Operation rollBackOperation(LevelDBWrapper levelDB) {
        String oldValue = levelDB.get(this.key);
        return OperationFactory.put(this.key, oldValue);
    }

    @Override
    public String toString() {
        return "del " + key;
    }
}
