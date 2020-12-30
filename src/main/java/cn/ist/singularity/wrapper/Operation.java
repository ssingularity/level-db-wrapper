package cn.ist.singularity.wrapper;

import java.lang.reflect.Field;

public class Operation {
    public Type type;
    public String key;
    public String value;

    public Operation(Type type, String key, String value) {
        this.type = type;
        this.key = key;
        this.value = value;
        switch (this.type) {
            case Delete:
            case Get:
                validateNotNull("key");
                return;
            case Put:
                validateNotNull("key", "value");
                return;
            default:
        }
    }

    private void validateNotNull(String... fieldNames) {
        try {
            for (String fieldName: fieldNames) {
                Field targetField = Operation.class.getDeclaredField(fieldName);
                String value = (String) targetField.get(this);
                if (value == null) {
                    throw new RuntimeException(fieldName + "不能为空");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
