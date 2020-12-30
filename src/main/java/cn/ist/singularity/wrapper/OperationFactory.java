package cn.ist.singularity.wrapper;

public class OperationFactory {
    public static Operation get(String key) {
        return new Operation(Type.Get, key, null);
    }

    public static Operation put(String key, String value) {
        return new Operation(Type.Put, key, value);
    }

    public static Operation delete(String key) {
        return new Operation(Type.Delete, key, null);
    }
}
