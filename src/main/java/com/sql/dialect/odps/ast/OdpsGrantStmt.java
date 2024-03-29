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
package com.sql.dialect.odps.ast;

import java.util.ArrayList;
import java.util.List;

import com.sql.ast.SQLExpr;
import com.sql.ast.SQLName;
import com.sql.ast.SQLObject;
import com.sql.ast.statement.SQLGrantStatement;
import com.sql.ast.statement.SQLObjectType;
import com.sql.dialect.odps.visitor.OdpsASTVisitor;
import com.sql.visitor.SQLASTVisitor;
import com.util.JdbcConstants;

public class OdpsGrantStmt extends SQLGrantStatement {

    private SQLObjectType subjectType;

    private boolean       isSuper = false;

    private boolean       isLabel = false;
    private SQLExpr       label;
    private List<SQLName> columns = new ArrayList<SQLName>(); ;
    private SQLExpr       expire;

    public OdpsGrantStmt(){
        super(JdbcConstants.ODPS);
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        accept0((OdpsASTVisitor) visitor);
    }

    protected void accept0(OdpsASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, on);
            acceptChild(visitor, to);
        }
        visitor.endVisit(this);
    }

    public SQLObject getOn() {
        return on;
    }

    public void setOn(SQLExpr on) {
        if (on != null) {
            on.setParent(this);
        }
        this.on = on;
    }

    public SQLExpr getTo() {
        return to;
    }

    public void setTo(SQLExpr to) {
        this.to = to;
    }

    public List<SQLExpr> getPrivileges() {
        return privileges;
    }

    public SQLObjectType getSubjectType() {
        return subjectType;
    }

    public void setSubjectType(SQLObjectType subjectType) {
        this.subjectType = subjectType;
    }

    public boolean isSuper() {
        return isSuper;
    }

    public void setSuper(boolean isSuper) {
        this.isSuper = isSuper;
    }

    public boolean isLabel() {
        return isLabel;
    }

    public void setLabel(boolean isLabel) {
        this.isLabel = isLabel;
    }

    public SQLExpr getLabel() {
        return label;
    }

    public void setLabel(SQLExpr label) {
        this.label = label;
    }

    public List<SQLName> getColumns() {
        return columns;
    }

    public void setColumnList(List<SQLName> columns) {
        this.columns = columns;
    }

    public SQLExpr getExpire() {
        return expire;
    }

    public void setExpire(SQLExpr expire) {
        this.expire = expire;
    }

}
