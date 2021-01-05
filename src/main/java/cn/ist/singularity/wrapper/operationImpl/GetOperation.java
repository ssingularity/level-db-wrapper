package cn.ist.singularity.wrapper.operationImpl;

import cn.ist.singularity.wrapper.LevelDBWrapper;
import cn.ist.singularity.wrapper.Operation;

public class GetOperation extends Operation {
    public GetOperation(String key) {
        super(key);
    }

    @Override
    public String onVisit(LevelDBWrapper levelDB) {
        String value = levelDB.get(this.key);
        return "get " + key + " " + value;
    }

    @Override
    public Operation rollBackOperation(LevelDBWrapper levelDB) {
        return new NullOperation("");
    }

    @Override
    public String toString() {
        return "get " + key;
    }
}
