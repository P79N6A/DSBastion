/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sql.dialect.mysql.ast.statement;

import com.sql.ast.SQLExpr;
import com.sql.ast.SQLPartitionBy;
import com.sql.dialect.mysql.ast.MySqlObject;
import com.sql.dialect.mysql.visitor.MySqlASTVisitor;
import com.sql.visitor.SQLASTVisitor;

public class MySqlPartitionByKey extends SQLPartitionBy implements MySqlObject {
    private short algorithm = 2;

    public short getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(short algorithm) {
        this.algorithm = algorithm;
    }
    
    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor instanceof MySqlASTVisitor) {
            accept0((MySqlASTVisitor) visitor);
        } else {
            throw new IllegalArgumentException("not support visitor type : " + visitor.getClass().getName());
        }
    }
    
    @Override
    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, columns);
            acceptChild(visitor, partitionsCount);
            acceptChild(visitor, getPartitions());
            acceptChild(visitor, subPartitionBy);
        }
        visitor.endVisit(this);
    }

    public void cloneTo(MySqlPartitionByKey x) {
        super.cloneTo(x);
        for (SQLExpr column : columns) {
            SQLExpr c2 = column.clone();
            c2.setParent(x);
            x.columns.add(c2);
        }
	x.setAlgorithm(algorithm);
    }

    public MySqlPartitionByKey clone() {
        MySqlPartitionByKey x = new MySqlPartitionByKey();
        cloneTo(x);
        return x;
    }
}
