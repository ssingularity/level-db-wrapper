package cn.ist.singularity.wrapper.operationImpl;

import cn.ist.singularity.wrapper.LevelDBWrapper;
import cn.ist.singularity.wrapper.Operation;

/**
 * @Author: ssingualrity
 * @Date: 2020/12/30 15:58
 */
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
    public String toString() {
        return "get " + key;
    }
}
