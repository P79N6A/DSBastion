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
package com.sql.dialect.sqlserver.ast.stmt;

import com.sql.ast.statement.SQLUpdateStatement;
import com.sql.dialect.sqlserver.ast.SQLServerOutput;
import com.sql.dialect.sqlserver.ast.SQLServerStatement;
import com.sql.dialect.sqlserver.ast.SQLServerTop;
import com.sql.dialect.sqlserver.visitor.SQLServerASTVisitor;
import com.sql.visitor.SQLASTVisitor;
import com.util.JdbcConstants;

public class SQLServerUpdateStatement extends SQLUpdateStatement implements SQLServerStatement {

    private SQLServerTop    top;
    private SQLServerOutput output;
    
    public SQLServerUpdateStatement(){
        super (JdbcConstants.SQL_SERVER);
    }

    public SQLServerTop getTop() {
        return top;
    }

    public void setTop(SQLServerTop top) {
        if (top != null) {
            top.setParent(this);
        }
        this.top = top;
    }

    public SQLServerOutput getOutput() {
        return output;
    }

    public void setOutput(SQLServerOutput output) {
        if (output != null) {
            output.setParent(this);
        }
        this.output = output;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        this.accept0((SQLServerASTVisitor) visitor);
    }

    @Override
    public void accept0(SQLServerASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, top);
            acceptChild(visitor, tableSource);
            acceptChild(visitor, items);
            acceptChild(visitor, output);
            acceptChild(visitor, from);
            acceptChild(visitor, where);
        }
        visitor.endVisit(this);
    }

}
