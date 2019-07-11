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
package com.sql.parser;

import com.sql.ast.statement.SQLSelectQueryBlock;
import com.sql.dialect.db2.ast.stmt.DB2SelectQueryBlock;
import com.sql.dialect.db2.parser.DB2ExprParser;
import com.sql.dialect.db2.parser.DB2Lexer;
import com.sql.dialect.db2.parser.DB2StatementParser;
import com.sql.dialect.h2.parser.H2StatementParser;
import com.sql.dialect.hive.parser.HiveStatementParser;
import com.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.sql.dialect.mysql.parser.MySqlExprParser;
import com.sql.dialect.mysql.parser.MySqlLexer;
import com.sql.dialect.mysql.parser.MySqlStatementParser;
import com.sql.dialect.odps.parser.OdpsExprParser;
import com.sql.dialect.odps.parser.OdpsLexer;
import com.sql.dialect.odps.parser.OdpsStatementParser;
import com.sql.dialect.oracle.ast.stmt.OracleSelectQueryBlock;
import com.sql.dialect.oracle.parser.OracleExprParser;
import com.sql.dialect.oracle.parser.OracleLexer;
import com.sql.dialect.oracle.parser.OracleStatementParser;
import com.sql.dialect.phoenix.parser.PhoenixExprParser;
import com.sql.dialect.phoenix.parser.PhoenixLexer;
import com.sql.dialect.phoenix.parser.PhoenixStatementParser;
import com.sql.dialect.postgresql.parser.PGExprParser;
import com.sql.dialect.postgresql.parser.PGLexer;
import com.sql.dialect.postgresql.parser.PGSQLStatementParser;
import com.sql.dialect.sqlserver.parser.SQLServerExprParser;
import com.sql.dialect.sqlserver.parser.SQLServerLexer;
import com.sql.dialect.sqlserver.parser.SQLServerStatementParser;
import com.util.JdbcConstants;

public class SQLParserUtils {

    public static SQLStatementParser createSQLStatementParser(String sql, String dbType) {
        SQLParserFeature[] features;
        if (JdbcConstants.ODPS.equals(dbType) || JdbcConstants.MYSQL.equals(dbType)) {
            features = new SQLParserFeature[] {SQLParserFeature.KeepComments};
        } else {
            features = new SQLParserFeature[] {};
        }
        return createSQLStatementParser(sql, dbType, features);
    }

    public static SQLStatementParser createSQLStatementParser(String sql, String dbType, boolean keepComments) {
        SQLParserFeature[] features;
        if (keepComments) {
            features = new SQLParserFeature[] {SQLParserFeature.KeepComments};
        } else {
            features = new SQLParserFeature[] {};
        }

        return createSQLStatementParser(sql, dbType, features);
    }

    public static SQLStatementParser createSQLStatementParser(String sql, String dbType, SQLParserFeature... features) {
        if (JdbcConstants.ORACLE.equals(dbType) || JdbcConstants.ALI_ORACLE.equals(dbType)) {
            return new OracleStatementParser(sql);
        }

        if (JdbcConstants.MYSQL.equals(dbType) || JdbcConstants.ALIYUN_DRDS.equals(dbType)) {
            return new MySqlStatementParser(sql, features);
        }

        if (JdbcConstants.MARIADB.equals(dbType)) {
            return new MySqlStatementParser(sql, features);
        }

        if (JdbcConstants.POSTGRESQL.equals(dbType)
                || JdbcConstants.ENTERPRISEDB.equals(dbType)) {
            return new PGSQLStatementParser(sql);
        }

        if (JdbcConstants.SQL_SERVER.equals(dbType) || JdbcConstants.JTDS.equals(dbType)) {
            return new SQLServerStatementParser(sql);
        }

        if (JdbcConstants.H2.equals(dbType)) {
            return new H2StatementParser(sql);
        }
        
        if (JdbcConstants.DB2.equals(dbType)) {
            return new DB2StatementParser(sql);
        }
        
        if (JdbcConstants.ODPS.equals(dbType)) {
            return new OdpsStatementParser(sql);
        }

        if (JdbcConstants.PHOENIX.equals(dbType)) {
            return new PhoenixStatementParser(sql);
        }

        if (JdbcConstants.HIVE.equals(dbType)) {
            return new HiveStatementParser(sql);
        }

        if (JdbcConstants.ELASTIC_SEARCH.equals(dbType)) {
            return new MySqlStatementParser(sql);
        }

        return new SQLStatementParser(sql, dbType);
    }

    public static SQLExprParser createExprParser(String sql, String dbType) {
        if (JdbcConstants.ORACLE.equals(dbType) || JdbcConstants.ALI_ORACLE.equals(dbType)) {
            return new OracleExprParser(sql);
        }

        if (JdbcConstants.MYSQL.equals(dbType) || //
            JdbcConstants.MARIADB.equals(dbType) || //
            JdbcConstants.H2.equals(dbType)) {
            return new MySqlExprParser(sql);
        }

        if (JdbcConstants.POSTGRESQL.equals(dbType)
                || JdbcConstants.ENTERPRISEDB.equals(dbType)) {
            return new PGExprParser(sql);
        }

        if (JdbcConstants.SQL_SERVER.equals(dbType) || JdbcConstants.JTDS.equals(dbType)) {
            return new SQLServerExprParser(sql);
        }
        
        if (JdbcConstants.DB2.equals(dbType)) {
            return new DB2ExprParser(sql);
        }
        
        if (JdbcConstants.ODPS.equals(dbType)) {
            return new OdpsExprParser(sql);
        }

        if (JdbcConstants.PHOENIX.equals(dbType)) {
            return new PhoenixExprParser(sql);
        }

        return new SQLExprParser(sql);
    }

    public static Lexer createLexer(String sql, String dbType) {
        if (JdbcConstants.ORACLE.equals(dbType) || JdbcConstants.ALI_ORACLE.equals(dbType)) {
            return new OracleLexer(sql);
        }

        if (JdbcConstants.MYSQL.equals(dbType) || //
                JdbcConstants.MARIADB.equals(dbType) || //
                JdbcConstants.H2.equals(dbType)) {
            return new MySqlLexer(sql);
        }

        if (JdbcConstants.POSTGRESQL.equals(dbType)
                || JdbcConstants.ENTERPRISEDB.equals(dbType)) {
            return new PGLexer(sql);
        }

        if (JdbcConstants.SQL_SERVER.equals(dbType) || JdbcConstants.JTDS.equals(dbType)) {
            return new SQLServerLexer(sql);
        }

        if (JdbcConstants.DB2.equals(dbType)) {
            return new DB2Lexer(sql);
        }

        if (JdbcConstants.ODPS.equals(dbType)) {
            return new OdpsLexer(sql);
        }

        if (JdbcConstants.PHOENIX.equals(dbType)) {
            return new PhoenixLexer(sql);
        }

        return new Lexer(sql);
    }

    public static SQLSelectQueryBlock createSelectQueryBlock(String dbType) {
        if (JdbcConstants.MYSQL.equals(dbType)) {
            return new MySqlSelectQueryBlock();
        }

        if (JdbcConstants.ORACLE.equals(dbType)) {
            return new OracleSelectQueryBlock();
        }

        if (JdbcConstants.DB2.equals(dbType)) {
            return new DB2SelectQueryBlock();
        }

        if (JdbcConstants.POSTGRESQL.equals(dbType)) {
            return new DB2SelectQueryBlock();
        }

        if (JdbcConstants.ODPS.equals(dbType)) {
            return new DB2SelectQueryBlock();
        }

        if (JdbcConstants.SQL_SERVER.equals(dbType)) {
            return new DB2SelectQueryBlock();
        }

        return new SQLSelectQueryBlock();
     }
}
