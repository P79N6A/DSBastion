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

import com.sql.dialect.postgresql.ast.expr.PGBoxExpr;
import com.sql.dialect.postgresql.ast.expr.PGCidrExpr;
import com.sql.dialect.postgresql.ast.expr.PGCircleExpr;
import com.sql.dialect.postgresql.ast.expr.PGExtractExpr;
import com.sql.dialect.postgresql.ast.expr.PGInetExpr;
import com.sql.dialect.postgresql.ast.expr.PGLineSegmentsExpr;
import com.sql.dialect.postgresql.ast.expr.PGMacAddrExpr;
import com.sql.dialect.postgresql.ast.expr.PGPointExpr;
import com.sql.dialect.postgresql.ast.expr.PGPolygonExpr;
import com.sql.dialect.postgresql.ast.expr.PGTypeCastExpr;
import com.sql.dialect.postgresql.ast.stmt.PGConnectToStatement;
import com.sql.dialect.postgresql.ast.stmt.PGDeleteStatement;
import com.sql.dialect.postgresql.ast.stmt.PGFunctionTableSource;
import com.sql.dialect.postgresql.ast.stmt.PGInsertStatement;
import com.sql.dialect.postgresql.ast.stmt.PGSelectQueryBlock;
import com.sql.dialect.postgresql.ast.stmt.PGSelectStatement;
import com.sql.dialect.postgresql.ast.stmt.PGShowStatement;
import com.sql.dialect.postgresql.ast.stmt.PGStartTransactionStatement;
import com.sql.dialect.postgresql.ast.stmt.PGUpdateStatement;
import com.sql.dialect.postgresql.ast.stmt.PGValuesQuery;
import com.sql.visitor.SQLASTVisitor;

public interface PGASTVisitor extends SQLASTVisitor {

    void endVisit(PGSelectQueryBlock x);

    boolean visit(PGSelectQueryBlock x);

    void endVisit(PGSelectQueryBlock.WindowClause x);

    boolean visit(PGSelectQueryBlock.WindowClause x);

    void endVisit(PGSelectQueryBlock.FetchClause x);

    boolean visit(PGSelectQueryBlock.FetchClause x);

    void endVisit(PGSelectQueryBlock.ForClause x);

    boolean visit(PGSelectQueryBlock.ForClause x);

    void endVisit(PGDeleteStatement x);

    boolean visit(PGDeleteStatement x);

    void endVisit(PGInsertStatement x);

    boolean visit(PGInsertStatement x);

    void endVisit(PGSelectStatement x);

    boolean visit(PGSelectStatement x);

    void endVisit(PGUpdateStatement x);

    boolean visit(PGUpdateStatement x);

    void endVisit(PGFunctionTableSource x);

    boolean visit(PGFunctionTableSource x);
    
    void endVisit(PGTypeCastExpr x);
    
    boolean visit(PGTypeCastExpr x);
    
    void endVisit(PGValuesQuery x);
    
    boolean visit(PGValuesQuery x);
    
    void endVisit(PGExtractExpr x);
    
    boolean visit(PGExtractExpr x);
    
    void endVisit(PGBoxExpr x);
    
    boolean visit(PGBoxExpr x);
    
    void endVisit(PGPointExpr x);
    
    boolean visit(PGPointExpr x);
    
    void endVisit(PGMacAddrExpr x);
    
    boolean visit(PGMacAddrExpr x);
    
    void endVisit(PGInetExpr x);
    
    boolean visit(PGInetExpr x);
    
    void endVisit(PGCidrExpr x);
    
    boolean visit(PGCidrExpr x);
    
    void endVisit(PGPolygonExpr x);
    
    boolean visit(PGPolygonExpr x);
    
    void endVisit(PGCircleExpr x);
    
    boolean visit(PGCircleExpr x);
    
    void endVisit(PGLineSegmentsExpr x);
    
    boolean visit(PGLineSegmentsExpr x);

    void endVisit(PGShowStatement x);
    
    boolean visit(PGShowStatement x);

    void endVisit(PGStartTransactionStatement x);
    boolean visit(PGStartTransactionStatement x);

    void endVisit(PGConnectToStatement x);
    boolean visit(PGConnectToStatement x);

}
