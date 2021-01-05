package cn.ist.singularity.wrapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class Operations {
    private List<Operation> operationList = new ArrayList<>();

    public static Operations of(Operation... operations) {
        Operations res = new Operations();
        res.operationList = Arrays.asList(operations);
        return res;
    }

    public static Operations of(List<Operation> operations) {
        Operations res = new Operations();
        res.operationList = operations;
        return res;
    }

    @Override
    synchronized public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Operation operation: operationList) {
            sb.append(operation.toString());
            sb.append(",");
        }
        String res = sb.toString();
        return res.substring(0, res.length() - 1);
    }

    public List<Operation> intern() {
        return this.operationList;
    }

    public void forEach(Consumer<Operation> consumer) {
        this.operationList.forEach(consumer);
    }
}
