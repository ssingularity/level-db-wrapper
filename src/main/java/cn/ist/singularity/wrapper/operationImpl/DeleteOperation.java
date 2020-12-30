package cn.ist.singularity.wrapper.operationImpl;

import cn.ist.singularity.wrapper.LevelDBWrapper;
import cn.ist.singularity.wrapper.Operation;

/**
 * @Author: ssingualrity
 * @Date: 2020/12/30 15:59
 */
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
    public String toString() {
        return "del " + key;
    }
}
