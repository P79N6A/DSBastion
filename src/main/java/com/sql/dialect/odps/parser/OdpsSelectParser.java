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
package com.sql.dialect.odps.parser;

import com.sql.ast.SQLExpr;
import com.sql.ast.SQLLimit;
import com.sql.ast.SQLOrderingSpecification;
import com.sql.ast.SQLSetQuantifier;
import com.sql.ast.expr.SQLListExpr;
import com.sql.ast.statement.SQLSelectOrderByItem;
import com.sql.ast.statement.SQLSelectQuery;
import com.sql.ast.statement.SQLTableSource;
import com.sql.dialect.odps.ast.OdpsSelectQueryBlock;
import com.sql.dialect.odps.ast.OdpsValuesTableSource;
import com.sql.parser.SQLExprParser;
import com.sql.parser.SQLSelectListCache;
import com.sql.parser.SQLSelectParser;
import com.sql.parser.Token;

public class OdpsSelectParser extends SQLSelectParser {
    public OdpsSelectParser(SQLExprParser exprParser){
        super(exprParser.getLexer());
        this.exprParser = exprParser;
    }

    public OdpsSelectParser(SQLExprParser exprParser, SQLSelectListCache selectListCache){
        super(exprParser.getLexer());
        this.exprParser = exprParser;
        this.selectListCache = selectListCache;
    }

    @Override
    public SQLSelectQuery query() {
        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();

            SQLSelectQuery select = query();
            accept(Token.RPAREN);

            return queryRest(select);
        }

        OdpsSelectQueryBlock queryBlock = new OdpsSelectQueryBlock();
        
        if (lexer.hasComment() && lexer.isKeepComments()) {
            queryBlock.addBeforeComment(lexer.readAndResetComments());
        }
        
        accept(Token.SELECT);
        
        if (lexer.token() == Token.HINT) {
            this.exprParser.parseHints(queryBlock.getHints());
        }

        if (lexer.token() == Token.COMMENT) {
            lexer.nextToken();
        }

        if (lexer.token() == Token.DISTINCT) {
            queryBlock.setDistionOption(SQLSetQuantifier.DISTINCT);
            lexer.nextToken();
        } else if (lexer.token() == Token.UNIQUE) {
            queryBlock.setDistionOption(SQLSetQuantifier.UNIQUE);
            lexer.nextToken();
        } else if (lexer.token() == Token.ALL) {
            queryBlock.setDistionOption(SQLSetQuantifier.ALL);
            lexer.nextToken();
        }

        parseSelectList(queryBlock);

        parseFrom(queryBlock);

        parseWhere(queryBlock);

        parseGroupBy(queryBlock);

        queryBlock.setOrderBy(this.exprParser.parseOrderBy());
        
        if (lexer.token() == Token.DISTRIBUTE) {
            lexer.nextToken();
            accept(Token.BY);
            this.exprParser.exprList(queryBlock.getDistributeBy(), queryBlock);

            if (lexer.identifierEquals("SORT")) {
                lexer.nextToken();
                accept(Token.BY);
                
                for (;;) {
                    SQLExpr expr = this.expr();
                    
                    SQLSelectOrderByItem sortByItem = new SQLSelectOrderByItem(expr);
                    
                    if (lexer.token() == Token.ASC) {
                        sortByItem.setType(SQLOrderingSpecification.ASC);
                        lexer.nextToken();
                    } else if (lexer.token() == Token.DESC) {
                        sortByItem.setType(SQLOrderingSpecification.DESC);
                        lexer.nextToken();
                    }
                    
                    queryBlock.getSortBy().add(sortByItem);
                    
                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                    } else {
                        break;
                    }
                }
            }
        }

        if (lexer.token() == Token.LIMIT) {
            lexer.nextToken();
            queryBlock.setLimit(new SQLLimit(this.expr()));
        }

        return queryRest(queryBlock);
    }

    public SQLTableSource parseTableSource() {
        if (lexer.token() == Token.VALUES) {
            lexer.nextToken();
            OdpsValuesTableSource tableSource = new OdpsValuesTableSource();

            for (;;) {
                accept(Token.LPAREN);
                SQLListExpr listExpr = new SQLListExpr();
                this.exprParser.exprList(listExpr.getItems(), listExpr);
                accept(Token.RPAREN);

                listExpr.setParent(tableSource);

                tableSource.getValues().add(listExpr);

                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }
                break;
            }

            String alias = this.tableAlias();
            tableSource.setAlias(alias);

            accept(Token.LPAREN);
            this.exprParser.names(tableSource.getColumns(), tableSource);
            accept(Token.RPAREN);

            return tableSource;
        }

        return super.parseTableSource();
    }
}
