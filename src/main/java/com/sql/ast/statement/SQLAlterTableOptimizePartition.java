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

import com.sql.ast.SQLName;
import com.sql.ast.SQLObjectImpl;
import com.sql.visitor.SQLASTVisitor;

public class SQLAlterTableOptimizePartition extends SQLObjectImpl implements SQLAlterTableItem {

    private final List<SQLName> partitions = new ArrayList<SQLName>(4);

    public List<SQLName> getPartitions() {
        return partitions;
    }
    
    public void addPartition(SQLName partition) {
        if (partition != null) {
            partition.setParent(this);
        }
        this.partitions.add(partition);
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, partitions);
        }
        visitor.endVisit(this);
    }
}
