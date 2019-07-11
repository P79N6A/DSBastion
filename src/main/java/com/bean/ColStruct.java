package com.bean;

import java.util.Objects;

public class ColStruct {

    private final String operator;
    private final TableStruct table;
    private final String name;
    private final String alias;
    private boolean visible = true;
    private MaskBean maskBean;

    public ColStruct(TableStruct table, String name, String operator) {
        this(table, name, name, operator);
    }

    public ColStruct(TableStruct table, String name, String alias, String operator) {
        this.table = table;
        this.name = name;
        this.alias = alias == null ? name : alias;
        this.operator = operator;
    }

    public TableStruct getTable() {
        return table;
    }

    public String getOperator() {
        return operator;
    }

    public String getSchema_catalog() {
        return table.getSchema_catalog();
    }

    public String getT_name() {
        return table.getName();
    }

    public String getT_alias() {
        return table.getAlias();
    }

    public String getName() {
        return name;
    }

    public String getAlias() {
        return alias;
    }

    public void invisible() {
        this.visible = false;
    }

    public boolean isVisible() {
        return visible;
    }

    public MaskBean getMaskBean() {
        return maskBean;
    }

    public void setMaskBean(MaskBean maskBean) {
        this.maskBean = maskBean;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColStruct other = (ColStruct) o;
        return operator.equals(other.operator) &&
                table.equals(other.table) &&
                (name.equals(other.name) ||
                        alias.equals(other.name));

    }

    @Override
    public int hashCode() {
        return Objects.hash(operator, table, name, alias);
    }

    @Override
    public String toString() {
        return table.toString() + "." + name;
    }
}
