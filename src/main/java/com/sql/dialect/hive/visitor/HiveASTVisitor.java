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

import com.sql.dialect.hive.ast.HiveInsert;
import com.sql.dialect.hive.ast.HiveInsertStatement;
import com.sql.dialect.hive.ast.HiveMultiInsertStatement;
import com.sql.dialect.hive.stmt.HiveCreateTableStatement;
import com.sql.visitor.SQLASTVisitor;

public interface HiveASTVisitor extends SQLASTVisitor {
    boolean visit(HiveCreateTableStatement x);
    void endVisit(HiveCreateTableStatement x);

    boolean visit(HiveMultiInsertStatement x);
    void endVisit(HiveMultiInsertStatement x);

    boolean visit(HiveInsertStatement x);
    void endVisit(HiveInsertStatement x);

    boolean visit(HiveInsert x);
    void endVisit(HiveInsert x);
}
