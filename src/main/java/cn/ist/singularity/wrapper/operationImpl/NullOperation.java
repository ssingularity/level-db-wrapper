package cn.ist.singularity.wrapper.operationImpl;

import cn.ist.singularity.wrapper.LevelDBWrapper;
import cn.ist.singularity.wrapper.Operation;

public class NullOperation extends Operation {
    public NullOperation(String key) {
        super(key);
    }

    @Override
    public String onVisit(LevelDBWrapper levelDB) {
        return "";
    }

    @Override
    public Operation rollBackOperation(LevelDBWrapper levelDB) {
        return new NullOperation("");
    }
}
