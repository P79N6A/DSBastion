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
package com.sql.dialect.sqlserver.visitor;


/*public class SQLServerSchemaStatVisitor extends SchemaStatVisitor implements SQLServerASTVisitor {
    public SQLServerSchemaStatVisitor() {
        super(JdbcConstants.SQL_SERVER);
    }

    @Override
    public boolean visit(SQLServerSelectQueryBlock x) {
        return visit((SQLSelectQueryBlock) x);
    }

    @Override
    public void endVisit(SQLServerSelectQueryBlock x) {
        endVisit((SQLSelectQueryBlock) x);
    }

    @Override
    public boolean visit(SQLServerTop x) {
        return false;
    }

    @Override
    public void endVisit(SQLServerTop x) {

    }

    @Override
    public boolean visit(SQLServerObjectReferenceExpr x) {
        return false;
    }

    @Override
    public void endVisit(SQLServerObjectReferenceExpr x) {

    }

    @Override
    public boolean visit(SQLServerInsertStatement x) {
        this.visit((SQLInsertStatement) x);
        return false;
    }

    @Override
    public void endVisit(SQLServerInsertStatement x) {
        this.endVisit((SQLInsertStatement) x);
    }

    @Override
    public boolean visit(SQLServerUpdateStatement x) {
        TableStat stat = getTableStat(x.getTableName());
        stat.incrementUpdateCount();

        accept(x.getItems());
        accept(x.getFrom());
        accept(x.getWhere());

        return false;
    }

    @Override
    public void endVisit(SQLServerUpdateStatement x) {

    }

    @Override
    public boolean visit(SQLServerExecStatement x) {
        return false;
    }

    @Override
    public void endVisit(SQLServerExecStatement x) {

    }

    @Override
    public boolean visit(SQLServerSetTransactionIsolationLevelStatement x) {
        return false;
    }

    @Override
    public void endVisit(SQLServerSetTransactionIsolationLevelStatement x) {

    }

    @Override
    public boolean visit(SQLServerOutput x) {
        return false;
    }

    @Override
    public void endVisit(SQLServerOutput x) {

    }

    @Override
    public boolean visit(SQLServerRollbackStatement x) {
        return true;
    }

    @Override
    public void endVisit(SQLServerRollbackStatement x) {

    }

    @Override
    public boolean visit(SQLServerWaitForStatement x) {
        return true;
    }

    @Override
    public void endVisit(SQLServerWaitForStatement x) {

    }

	@Override
	public boolean visit(SQLServerParameter x) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void endVisit(SQLServerParameter x) {
		// TODO Auto-generated method stub
		
	}

}*/
