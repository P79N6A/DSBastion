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

import com.sql.ast.statement.SQLTableConstraint;

public class SQLDDLParser extends SQLStatementParser {

    public SQLDDLParser(String sql){
        super(sql);
    }

    public SQLDDLParser(SQLExprParser exprParser){
        super(exprParser);
    }

    protected SQLTableConstraint parseConstraint() {
        if (lexer.token == Token.CONSTRAINT) {
            lexer.nextToken();
        }

        if (lexer.token == Token.IDENTIFIER) {
            this.exprParser.name();
            throw new ParserException("TODO. " + lexer.info());
        }

        if (lexer.token == Token.PRIMARY) {
            lexer.nextToken();
            accept(Token.KEY);

            throw new ParserException("TODO. " + lexer.info());
        }

        throw new ParserException("TODO " + lexer.info());
    }
}
