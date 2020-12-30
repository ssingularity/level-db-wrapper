package cn.ist.singularity.wrapper;

public class Operation {
    public static enum Type {
        Delete, Get, Put
    }

    public Type type;

    public String key;

    public String value;

    public Operation(Type type, String key, String value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }
}
