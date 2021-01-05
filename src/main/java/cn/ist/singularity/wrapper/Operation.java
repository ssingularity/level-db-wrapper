package cn.ist.singularity.wrapper;

public abstract class Operation {
    public String key;

    public Operation(String key) {
        this.key = key;
        if (key == null) {
            throw new RuntimeException("键不能为空");
        }
    }

    abstract public String onVisit(LevelDBWrapper levelDB);

    abstract public Operation rollBackOperation(LevelDBWrapper levelDB);
}
