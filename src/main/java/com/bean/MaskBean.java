package com.bean;

public class MaskBean {

    private final String name;  //unique mask name
    private final int type;     //1:mask,2:encrypt
    private final String valueType;
    private final String param;   //encrypt key

    public MaskBean(String name, int type, String valueType, String param) {
        this.name = name;
        this.type = type;
        this.valueType = valueType;
        this.param = param;
    }

    public String getName() {
        return name;
    }

    public int getType() {
        return type;
    }

    public String getValueType() {
        return valueType;
    }

    public String getParam() {
        return param;
    }
}
