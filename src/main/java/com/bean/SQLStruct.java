package com.bean;


import java.util.*;

public class SQLStruct {

    private final String action;
    private final String dbType;
    private List<ColStruct> first;
    private List<ColStruct> other;

    public SQLStruct(String action, String dbType) {
        this.action = action;
        this.dbType = dbType;
    }

    public void addFirst(List<ColStruct> list) {
        for (ColStruct first : list) {
            addFirst(first);
        }
    }

    public void addFirst(ColStruct first) {
        if (this.first == null) this.first = new ArrayList<>();
        boolean add = true;
        for (ColStruct col : this.first) {
            if (col.equals(first)) {
                add = false;
                break;
            }
        }
        if (add) this.first.add(first);
    }

    public void addOther(List<ColStruct> others) {
        for (ColStruct other : others) {
            addOther(other);
        }
    }

    public void addOther(ColStruct other) {
        if (this.other == null) this.other = new ArrayList<>();
        boolean add = true;
        for (ColStruct col : this.first) {
            if (col.equals(other)) {
                add = false;
                break;
            }
        }
        if (add) for (ColStruct col : this.other) {
            if (col.equals(other)) {
                add = false;
                break;
            }
        }
        if (add) this.other.add(other);
    }

    public String getAction() {
        return action;
    }

    public String getDbType() {
        return dbType;
    }

    public List<ColStruct> getFirst() {
        return first;
    }

    public List<ColStruct> getOther() {
        return other;
    }
}

