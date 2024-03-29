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
package com.sql.ast.expr;

import java.util.Collections;
import java.util.List;

import com.sql.SQLUtils;
import com.sql.ast.SQLDataType;
import com.sql.ast.SQLObject;
import com.sql.ast.statement.SQLCharacterDataType;
import com.sql.visitor.SQLASTOutputVisitor;
import com.sql.visitor.SQLASTVisitor;

public class SQLCharExpr extends SQLTextLiteralExpr implements SQLValuableExpr{
    public static final SQLDataType DEFAULT_DATA_TYPE = new SQLCharacterDataType("varchar");

    public SQLCharExpr(){

    }

    public SQLCharExpr(String text){
        super(text);
    }

    @Override
    public void output(StringBuffer buf) {
        output((Appendable) buf);
    }

    public void output(Appendable buf) {
        this.accept(new SQLASTOutputVisitor(buf));
    }

    protected void accept0(SQLASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    @Override
    public Object getValue() {
        return this.text;
    }
    
    public String toString() {
        return SQLUtils.toSQLString(this);
    }

    public SQLCharExpr clone() {
        return new SQLCharExpr(this.text);
    }

    public SQLDataType computeDataType() {
        return DEFAULT_DATA_TYPE;
    }

    public List<SQLObject> getChildren() {
        return Collections.emptyList();
    }
}
