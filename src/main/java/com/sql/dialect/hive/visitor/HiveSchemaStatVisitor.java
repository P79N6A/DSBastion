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
package com.sql.dialect.hive.visitor;


/*public class HiveSchemaStatVisitor extends SchemaStatVisitor implements HiveASTVisitor {

    public HiveSchemaStatVisitor() {
        super (JdbcConstants.HIVE);
    }

    @Override
    public boolean visit(HiveCreateTableStatement x) {
        return super.visit((SQLCreateTableStatement) x);
    }

    @Override
    public void endVisit(HiveCreateTableStatement x) {

    }

    @Override
    public boolean visit(HiveInsert x) {
        setMode(x, TableStat.Mode.Insert);

        SQLExprTableSource tableSource = x.getTableSource();
        SQLExpr tableName = tableSource.getExpr();

        if (tableName instanceof SQLName) {
            TableStat stat = getTableStat((SQLName) tableName);
            stat.incrementInsertCount();

        }

        for (SQLAssignItem partition : x.getPartitions()) {
            partition.accept(this);
        }

        accept(x.getQuery());

        return false;
    }

    @Override
    public void endVisit(HiveInsert x) {

    }

    @Override
    public void endVisit(HiveMultiInsertStatement x) {

    }

    @Override
    public boolean visit(HiveMultiInsertStatement x) {
        if (repository != null
                && x.getParent() == null) {
            repository.resolve(x);
        }
        return true;
    }


    @Override
    public void endVisit(HiveInsertStatement x) {

    }

    @Override
    public boolean visit(HiveInsertStatement x) {
        if (repository != null
                && x.getParent() == null) {
            repository.resolve(x);
        }

        setMode(x, TableStat.Mode.Insert);

        SQLExprTableSource tableSource = x.getTableSource();
        SQLExpr tableName = tableSource.getExpr();

        if (tableName instanceof SQLName) {
            TableStat stat = getTableStat((SQLName) tableName);
            stat.incrementInsertCount();

        }

        for (SQLAssignItem partition : x.getPartitions()) {
            partition.accept(this);
        }

        accept(x.getQuery());

        return false;
    }

}*/
