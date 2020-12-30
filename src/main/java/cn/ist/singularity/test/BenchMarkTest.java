package cn.ist.singularity.test;

import cn.ist.singularity.impl.BaseLevelDBWrapper;
import cn.ist.singularity.wrapper.LevelDBWrapper;
import cn.ist.singularity.wrapper.Operation;
import cn.ist.singularity.wrapper.OperationFactory;

import java.util.ArrayList;
import java.util.List;

public class BenchMarkTest {
    public static void main(String[] args) {
        LevelDBWrapper levelDB = new BaseLevelDBWrapper("E:\\db");
        List<Operation> operationList = new ArrayList<>();
        operationList.add(OperationFactory.put("test", "test"));
        operationList.add(OperationFactory.get("test"));
        operationList.add(OperationFactory.delete("test"));
        operationList.add(OperationFactory.get("test"));
        levelDB.batch(operationList);
    }
}
