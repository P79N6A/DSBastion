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
package com.sql.dialect.postgresql.visitor;

import java.util.ArrayList;
import java.util.List;

import com.sql.ast.SQLOrderBy;
import com.sql.ast.expr.SQLBetweenExpr;
import com.sql.ast.expr.SQLBinaryOpExpr;
import com.sql.ast.expr.SQLInListExpr;
import com.sql.ast.expr.SQLMethodInvokeExpr;
import com.sql.ast.statement.SQLSelectGroupByClause;
import com.sql.ast.statement.SQLSelectItem;
import com.sql.visitor.ExportParameterVisitor;
import com.sql.visitor.ExportParameterVisitorUtils;

public class PGExportParameterVisitor extends PGOutputVisitor implements ExportParameterVisitor {

    /**
     * true= if require parameterized sql output
     */
    private final boolean requireParameterizedOutput;

    public PGExportParameterVisitor(final List<Object> parameters,final Appendable appender,final boolean wantParameterizedOutput){
        super(appender, true);
        this.parameters = parameters;
        this.requireParameterizedOutput = wantParameterizedOutput;
    }

    public PGExportParameterVisitor() {
        this(new ArrayList<Object>());
    }

    public PGExportParameterVisitor(final List<Object> parameters){
        this(parameters,new StringBuilder(),false);
    }

    public PGExportParameterVisitor(final Appendable appender) {
        this(new ArrayList<Object>(),appender,true);
    }
    
    public List<Object> getParameters() {
        return parameters;
    }

    @Override
    public boolean visit(SQLSelectItem x) {
        if(requireParameterizedOutput){
            return super.visit(x);
        }
        return false;
    }

    @Override
    public boolean visit(SQLOrderBy x) {
        if(requireParameterizedOutput){
            return super.visit(x);
        }
        return false;
    }

    @Override
    public boolean visit(SQLSelectGroupByClause x) {
        if(requireParameterizedOutput){
            return super.visit(x);
        }
        return false;
    }

    @Override
    public boolean visit(SQLMethodInvokeExpr x) {
        if(requireParameterizedOutput){
            return super.visit(x);
        }
        ExportParameterVisitorUtils.exportParamterAndAccept(this.parameters, x.getParameters());

        return true;
    }

    @Override
    public boolean visit(SQLInListExpr x) {
        if(requireParameterizedOutput){
            return super.visit(x);
        }
        ExportParameterVisitorUtils.exportParamterAndAccept(this.parameters, x.getTargetList());

        return true;
    }

    @Override
    public boolean visit(SQLBetweenExpr x) {
        if(requireParameterizedOutput){
            return super.visit(x);
        }
        ExportParameterVisitorUtils.exportParameter(this.parameters, x);
        return true;
    }

    public boolean visit(SQLBinaryOpExpr x) {
        if(requireParameterizedOutput){
            return super.visit(x);
        }
        ExportParameterVisitorUtils.exportParameter(this.parameters, x);
        return true;
    }

}
