package com.bean;

import java.util.Objects;

public class TableStruct {

    private final String schema_catalog;
    private final String name;
    private final String alias;

    public TableStruct(String schema_catalog, String name) {
        this(schema_catalog, name, name);
    }

    public TableStruct(String schema_catalog, String name, String alias) {
        this.schema_catalog = schema_catalog;
        this.name = name;
        this.alias = alias == null ? name : alias;
    }

    public String getSchema_catalog() {
        return schema_catalog;
    }

    public String getName() {
        return name;
    }

    public String getAlias() {
        return alias;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableStruct other = (TableStruct) o;
        return schema_catalog.equals(other.schema_catalog) &&
                name.equals(other.name) &&
                alias.equals(other.alias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema_catalog, name, alias);
    }

    public boolean equals(String schema_catalog, String str) {
        return this.schema_catalog.equals(schema_catalog) &&
                (this.name.equals(str) || this.alias.equals(str));
    }

    @Override
    public String toString() {
        return schema_catalog + "." + name;
    }
}
