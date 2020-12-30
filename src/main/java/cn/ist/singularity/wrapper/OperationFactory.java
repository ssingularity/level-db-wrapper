package cn.ist.singularity.wrapper;

import cn.ist.singularity.wrapper.operationImpl.DeleteOperation;
import cn.ist.singularity.wrapper.operationImpl.GetOperation;
import cn.ist.singularity.wrapper.operationImpl.PutOperation;

public class OperationFactory {
    public static Operation get(String key) {
        return new GetOperation(key);
    }

    public static Operation put(String key, String value) {
        return new PutOperation(key, value);
    }

    public static Operation delete(String key) {
        return new DeleteOperation(key);
    }
}
