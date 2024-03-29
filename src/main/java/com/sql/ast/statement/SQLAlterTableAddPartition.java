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
package com.sql.ast.statement;

import java.util.ArrayList;
import java.util.List;

import com.sql.ast.SQLExpr;
import com.sql.ast.SQLObject;
import com.sql.ast.SQLObjectImpl;
import com.sql.visitor.SQLASTVisitor;

public class SQLAlterTableAddPartition extends SQLObjectImpl implements SQLAlterTableItem {

    private boolean               ifNotExists = false;

    private final List<SQLObject> partitions  = new ArrayList<SQLObject>(4);

    private SQLExpr               partitionCount;

    public List<SQLObject> getPartitions() {
        return partitions;
    }

    public void addPartition(SQLObject partition) {
        if (partition != null) {
            partition.setParent(this);
        }
        this.partitions.add(partition);
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public SQLExpr getPartitionCount() {
        return partitionCount;
    }

    public void setPartitionCount(SQLExpr partitionCount) {
        if (partitionCount != null) {
            partitionCount.setParent(this);
        }
        this.partitionCount = partitionCount;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, partitions);
        }
        visitor.endVisit(this);
    }
}
