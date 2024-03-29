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
package com.sql.builder.impl;

import com.sql.ast.SQLExpr;
import com.sql.ast.expr.SQLBooleanExpr;
import com.sql.ast.expr.SQLCharExpr;
import com.sql.ast.expr.SQLIntegerExpr;
import com.sql.ast.expr.SQLNullExpr;
import com.sql.ast.expr.SQLNumberExpr;
import com.sql.builder.SQLBuilder;


public class SQLBuilderImpl implements SQLBuilder {
    public static SQLExpr toSQLExpr(Object obj, String dbType) {
        if (obj == null) {
            return new SQLNullExpr();
        }
        
        if (obj instanceof Integer) {
            return new SQLIntegerExpr((Integer) obj);
        }
        
        if (obj instanceof Number) {
            return new SQLNumberExpr((Number) obj);
        }
        
        if (obj instanceof String) {
            return new SQLCharExpr((String) obj);
        }
        
        if (obj instanceof Boolean) {
            return new SQLBooleanExpr((Boolean) obj);
        }
        
        throw new IllegalArgumentException("not support : " + obj.getClass().getName());
    }
}
