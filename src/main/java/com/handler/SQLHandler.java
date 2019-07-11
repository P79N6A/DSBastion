package com.handler;

import com.bean.*;
import com.exception.NotSupportException;
import com.exception.SQLParseException;
import com.sql.ast.*;
import com.sql.ast.expr.*;
import com.sql.ast.statement.*;
import com.sql.parser.SQLParserUtils;
import com.sql.parser.SQLStatementParser;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

/**
 * select中的group by列会在前面查询中出现,having对组进行过滤,故select拆解中可跳过group by...having....
 */
public class SQLHandler {

    private final WrapConnect wrapConnect;
    private final String user;
    private final SQLStatementParser stmtParser;

    private SQLStruct sqlStruct;
    private String sql;  //[select,update,insert] maybe change original sql
    private Map<Integer, MaskBean> pstmtMask;

    public SQLHandler(WrapConnect wc, String user, String sql) {
        this.wrapConnect = wc;
        this.user = user;
        this.sql = sql;
        this.stmtParser = SQLParserUtils.createSQLStatementParser(sql, wrapConnect.getDbType());
    }

    public String getSql() {
        return this.sql;
    }

    public SQLStruct getSqlStruct() {
        return sqlStruct;
    }

    public Map<Integer, MaskBean> getPstmtMask() {
        return pstmtMask;
    }

    public void handler() throws Exception {
        List<SQLStatement> sqlStatements = this.stmtParser.parseStatementList();
        if (sqlStatements.size() > 1)
            throw new NotSupportException("sql[" + this.sql + "] too complicated and not support...");
        SQLStatement sqlStatement = sqlStatements.get(0);
        if (sqlStatement instanceof SQLSelectStatement) {
            this.sqlStruct = new SQLStruct("select", wrapConnect.getDbType());
            SQLSelectStatement stmt = (SQLSelectStatement) sqlStatement;
            parseSQLSelect(stmt.getSelect());
            //row filter
            List<ColStruct> first = this.sqlStruct.getFirst();
            Map<String, TableStruct> tables = new HashMap<>(1);
            for (ColStruct col : first) {
                String key = col.getTable().toString();
                if (!tables.containsKey(key)) tables.put(key, col.getTable());
            }
            List<SQLBinaryOpExpr> rowFilters = new ArrayList<>();
            for (TableStruct table : tables.values()) {
                rowFilters.addAll(filterRow(table));
            }
            for (SQLBinaryOpExpr rowFilter : rowFilters) {
                stmt.addWhere(rowFilter);
            }
            this.sql = stmt.toString();
            //add mask
            MaskHandler.fillMask(wrapConnect.getAK(), user, this.sqlStruct);

        } else if (sqlStatement instanceof SQLUpdateStatement) {
            this.sqlStruct = new SQLStruct("update", wrapConnect.getDbType());
            SQLUpdateStatement stmt = (SQLUpdateStatement) sqlStatement;
            List<SQLUpdateSetItem> updateSetItems = stmt.getItems();
            List<SQLExpr> first = new ArrayList<>(updateSetItems.size());
            List<SQLExpr> other = new ArrayList<>(updateSetItems.size());
            for (SQLUpdateSetItem item : updateSetItems) {
                first.add(item.getColumn());
                other.add(item.getValue());
            }
            SQLTableSource tableSource = stmt.getTableSource();
            if (tableSource instanceof SQLExprTableSource) {
                TableStruct table = parseSQLExprTableSource((SQLExprTableSource) tableSource);
                for (SQLExpr colExpr : first) {
                    ColStruct col = parseColumn("update", table, colExpr, null);
                    if (col != null) this.sqlStruct.addFirst(col);
                }
                for (SQLExpr valExpr : other) {
                    ColStruct val = parseColumn("update", table, valExpr, null);
                    if (val != null) this.sqlStruct.addOther(val);
                }
                //where
                parseSQLWhere(stmt.getWhere(), Collections.singletonList(table), null);

                //encrypt
                updateEncryption(other);
            } else if (tableSource instanceof SQLJoinTableSource) {
                List<TableStruct> tables = new ArrayList<>();
                parseSQLJoinTableSource((SQLJoinTableSource) tableSource, tables);
                Map<TableStruct, List<String>> colMapper = new HashMap<>(2);
                for (TableStruct table : tables) colMapper.put(table, parseSQLAllColumnExpr(table));
                for (SQLExpr colExpr : first) {
                    ColStruct col = parseColumn("update", tables, colExpr, null, colMapper);
                    if (col != null) this.sqlStruct.addFirst(col);
                }
                for (SQLExpr valExpr : other) {
                    ColStruct val = parseColumn("update", tables, valExpr, null, colMapper);
                    if (val != null) this.sqlStruct.addOther(val);
                }
                //where
                parseSQLWhere(stmt.getWhere(), tables, colMapper);

                //encrypt
                updateEncryption(other);
            } else throw new NotSupportException(this.sql, "update table", tableSource.getClass());
        } else if (sqlStatement instanceof SQLInsertStatement) {
            this.sqlStruct = new SQLStruct("insert", wrapConnect.getDbType());
            SQLInsertStatement stmt = (SQLInsertStatement) sqlStatement;
            TableStruct table = parseSQLExprTableSource(stmt.getTableSource());
            List<SQLExpr> i_col = stmt.getColumns();
            if (i_col.isEmpty()) {
                List<String> _cols = parseSQLAllColumnExpr(table);
                i_col = new ArrayList<>(_cols.size());
                for (String col : _cols) i_col.add(new SQLIdentifierExpr(col));
            }
            for (SQLExpr sqlExpr : i_col) {
                if (sqlExpr instanceof SQLIdentifierExpr) {
                    ColStruct colStruct = new ColStruct(table, parseSQLIdentifierExpr((SQLIdentifierExpr) sqlExpr),
                            "insert");
                    this.sqlStruct.addFirst(colStruct);
                } else if (sqlExpr instanceof SQLPropertyExpr) {
                    this.sqlStruct.addFirst(parseSQLPropertyExpr((SQLPropertyExpr) sqlExpr, Collections.singletonList(table),
                            null, "insert"));
                } else throw new NotSupportException(this.sql, "insert column", sqlExpr.getClass());
            }
            //encrypt
            for (SQLInsertStatement.ValuesClause valuesClause : stmt.getValuesList()) {
                insertEncryption(valuesClause);
            }
            this.sql = stmt.toString();

            parseSQLSelect(stmt.getQuery());
        } else if (sqlStatement instanceof SQLDeleteStatement) {
            this.sqlStruct = new SQLStruct("delete", wrapConnect.getDbType());
            SQLDeleteStatement stmt = (SQLDeleteStatement) sqlStatement;
            SQLTableSource tableSource = stmt.getTableSource();
            if (tableSource instanceof SQLExprTableSource) {
                TableStruct table = parseSQLExprTableSource((SQLExprTableSource) tableSource);
                this.sqlStruct.addFirst(new ColStruct(table, "", "delete"));
                //where
                parseSQLWhere(stmt.getWhere(), Collections.singletonList(table), null);
            } else throw new NotSupportException(this.sql, "delete table", tableSource.getClass());
        } else if (sqlStatement instanceof SQLAlterTableStatement) {
            this.sqlStruct = new SQLStruct("alter", wrapConnect.getDbType());
            SQLAlterTableStatement stmt = (SQLAlterTableStatement) sqlStatement;
            TableStruct table = parseSQLExprTableSource(stmt.getTableSource());
            ColStruct colStruct = new ColStruct(table, "", "alter");
            this.sqlStruct.addFirst(colStruct);
        } else if (sqlStatement instanceof SQLTruncateStatement) {
            this.sqlStruct = new SQLStruct("truncate", wrapConnect.getDbType());
            SQLTruncateStatement stmt = (SQLTruncateStatement) sqlStatement;
            for (SQLExprTableSource tableSource : stmt.getTableSources()) {
                TableStruct table = parseSQLExprTableSource(tableSource);
                ColStruct colStruct = new ColStruct(table, "", "truncate");
                this.sqlStruct.addFirst(colStruct);
            }
        } else if (sqlStatement instanceof SQLCreateTableStatement) {
            this.sqlStruct = new SQLStruct("create", wrapConnect.getDbType());
            SQLCreateTableStatement stmt = (SQLCreateTableStatement) sqlStatement;
            SQLExprTableSource tableSource = stmt.getTableSource();
            TableStruct table = parseSQLExprTableSource(tableSource);
            ColStruct colStruct = new ColStruct(table, "", "create");
            this.sqlStruct.addFirst(colStruct);
            parseSQLSelect(stmt.getSelect());
        } else if (sqlStatement instanceof SQLDropTableStatement) {
            this.sqlStruct = new SQLStruct("drop", wrapConnect.getDbType());
            SQLDropTableStatement stmt = (SQLDropTableStatement) sqlStatement;
            for (SQLExprTableSource tableSource : stmt.getTableSources()) {
                TableStruct table = parseSQLExprTableSource(tableSource);
                ColStruct colStruct = new ColStruct(table, "", "drop");
                this.sqlStruct.addFirst(colStruct);
            }
        } else throw new NotSupportException(this.sql, sqlStatement.getClass());

    }

    private void insertEncryption(SQLInsertStatement.ValuesClause valuesClause) throws Exception {
        MaskHandler.fillMask(wrapConnect.getAK(), user, this.sqlStruct);
        for (int i = 0; i < this.sqlStruct.getFirst().size(); i++) {
            ColStruct col = this.sqlStruct.getFirst().get(i);
            if (col.getMaskBean() != null && 2 == col.getMaskBean().getType()) {
                SQLExpr vexpr = valuesClause.getValues().get(i);
                if (vexpr instanceof SQLVariantRefExpr) { //pstmt
                    if (this.pstmtMask == null) this.pstmtMask = new HashMap<>();
                    this.pstmtMask.put(i + 1, col.getMaskBean());
                } else if (vexpr instanceof SQLCharExpr) {
                    String value = ((SQLCharExpr) vexpr).getText();
                    byte[] encrypt = CipherHandler.encrypt(value.getBytes(StandardCharsets.UTF_8),
                            col.getMaskBean());
                    String enVal = new String(encrypt, StandardCharsets.UTF_8);
                    valuesClause.getValues().set(i, new SQLCharExpr(enVal));
                }
            }
        }
    }

    private void updateEncryption(List<SQLExpr> values) throws Exception {
        MaskHandler.fillMask(wrapConnect.getAK(), user, this.sqlStruct);
        for (int i = 0; i < this.sqlStruct.getFirst().size(); i++) {
            ColStruct col = this.sqlStruct.getFirst().get(i);
            if (col.getMaskBean() != null && 2 == col.getMaskBean().getType()) {
                SQLExpr vexpr = values.get(i);
                if (vexpr instanceof SQLVariantRefExpr) { //pstmt
                    if (this.pstmtMask == null) this.pstmtMask = new HashMap<>();
                    this.pstmtMask.put(i + 1, col.getMaskBean());
                } else if (vexpr instanceof SQLCharExpr) {
                    String value = ((SQLCharExpr) vexpr).getText();
                    int start = this.sql.indexOf(col.getName()) + col.getName().length();
                    while (true) {
                        if (value.equals(this.sql.substring(start, start + value.length()))) break;
                        else start += 1;
                    }
                    byte[] encrypt = CipherHandler.encrypt(value.getBytes(StandardCharsets.UTF_8),
                            col.getMaskBean());
                    this.sql = this.sql.substring(0, start) + new String(encrypt, StandardCharsets.UTF_8)
                            + this.sql.substring(start + value.length());
                }
            }
        }
    }

    private void parseSQLSelect(SQLSelect sqlSelect) throws NotSupportException, SQLException {
        if (sqlSelect == null) return;
        SQLSelectQuery query = sqlSelect.getQuery();
        if (query instanceof SQLSelectQueryBlock) {
            SQLSelectQueryBlock selectQueryBlock = (SQLSelectQueryBlock) query;
            SQLTableSource tableSource = selectQueryBlock.getFrom();
            List<SQLSelectItem> selects = selectQueryBlock.getSelectList();
            assert selects != null;
            if (tableSource instanceof SQLExprTableSource) {
                TableStruct table = parseSQLExprTableSource((SQLExprTableSource) tableSource);
                List<ColStruct> list = new ArrayList<>();
                //query *
                if (selects.size() == 1 && selects.get(0).getExpr() instanceof SQLAllColumnExpr) {
                    List<String> cols = parseSQLAllColumnExpr(table);
                    cols.forEach(col -> {
                        ColStruct colStruct = new ColStruct(table, col, "select");
                        list.add(colStruct);
                    });
                } else for (SQLSelectItem item : selects) {
                    ColStruct col = parseColumn("select", table, item.getExpr(), item.getAlias());
                    if (col != null) list.add(col);
                }
                if (this.sqlStruct.getFirst() == null) this.sqlStruct.addFirst(list);
                else this.sqlStruct.addOther(list);
                //order by
                this.sqlStruct.addOther(parseOrderBy(selectQueryBlock.getOrderBy(),
                        Collections.singletonList(table), null));
                //where
                parseSQLWhere(selectQueryBlock.getWhere(), Collections.singletonList(table), null);
            } else if (tableSource instanceof SQLJoinTableSource) {
                List<TableStruct> tables = new ArrayList<>();
                parseSQLJoinTableSource((SQLJoinTableSource) tableSource, tables);
                Map<TableStruct, List<String>> colMapper = new HashMap<>(2);
                for (TableStruct table : tables) colMapper.put(table, parseSQLAllColumnExpr(table));
                List<ColStruct> list = new ArrayList<>();
                //query *
                if (selects.size() == 1 && selects.get(0).getExpr() instanceof SQLAllColumnExpr) {
                    for (TableStruct table : tables) {
                        List<String> cols = colMapper.get(table);
                        cols.forEach(col -> {
                            ColStruct colStruct = new ColStruct(table, col, "select");
                            list.add(colStruct);
                        });
                    }
                } else {
                    for (SQLSelectItem item : selects) {
                        ColStruct col = parseColumn("select", tables, item.getExpr(), item.getAlias(), colMapper);
                        if (col != null) list.add(col);
                    }
                }
                if (this.sqlStruct.getFirst() == null) this.sqlStruct.addFirst(list);
                else this.sqlStruct.addOther(list);
                //order by
                this.sqlStruct.addOther(parseOrderBy(selectQueryBlock.getOrderBy(), tables, colMapper));
                //where
                parseSQLWhere(selectQueryBlock.getWhere(), tables, colMapper);
            } else if (tableSource instanceof SQLSubqueryTableSource) {
                SQLSelect _select = ((SQLSubqueryTableSource) tableSource).getSelect();
                parseSQLSelect(_select);
                //avoid inner table selects more than actual selects
                if (this.sqlStruct.getFirst() != null && "select".equals(this.sqlStruct.getAction()) &&
                        !(selects.size() == 1 && selects.get(0).getExpr() instanceof SQLAllColumnExpr)) {
                    List<ColStruct> al = new ArrayList<>();
                    for (SQLSelectItem item : selects) {
                        String schema_catalog = null, tableName = null, colName;
//                        String alias = item.getAlias();
                        SQLExpr colExpr = item.getExpr();
                        if (colExpr instanceof SQLIdentifierExpr) {
                            colName = parseSQLIdentifierExpr((SQLIdentifierExpr) colExpr);
                        } else if (colExpr instanceof SQLPropertyExpr) {
                            String[] s = parseSQLPropertyExpr((SQLPropertyExpr) colExpr);
                            colName = s[s.length - 1];
                            if (s.length == 2) tableName = s[0];
                            if (s.length == 3) {
                                schema_catalog = s[0];
                                tableName = s[1];
                            }
                        } else
                            throw new NotSupportException(this.sql, "SQLSubqueryTableSource selects", colExpr.getClass());
                        for (ColStruct colStruct : this.sqlStruct.getFirst()) {
                            if (schema_catalog != null && colStruct.getSchema_catalog().equals(schema_catalog) &&
                                    (colStruct.getT_name().equals(tableName) ||
                                            colStruct.getT_alias().equals(tableName)) &&
                                    (colStruct.getName().equals(colName) || colStruct.getAlias().equals(colName))) {
                                al.add(colStruct);
                            } else if (tableName != null && (colStruct.getT_name().equals(tableName) ||
                                    colStruct.getT_alias().equals(tableName)) &&
                                    (colStruct.getName().equals(colName) || colStruct.getAlias().equals(colName))) {
                                al.add(colStruct);
                            } else if (colStruct.getName().equals(colName) || colStruct.getAlias().equals(colName)) {
                                al.add(colStruct);
                            }
                        }
                        this.sqlStruct.getFirst().clear();
                        this.sqlStruct.addFirst(al);
                    }
                }
            } else throw new NotSupportException(this.sql, "select table", tableSource.getClass());
        } else if (query instanceof SQLUnionQuery) {
            throw new NotSupportException(this.sql, "SQLUnionQuery", query.getClass());
        } else throw new NotSupportException(this.sql, "SQLSelectQuery", query.getClass());
    }


    private List<ColStruct> parseOrderBy(SQLOrderBy order, List<TableStruct> tables,
                                         Map<TableStruct, List<String>> colMapper) throws NotSupportException {
        List<ColStruct> list = new ArrayList<>();
        if (order != null) {
            assert tables.size() == 1 || colMapper != null;
            List<SQLSelectOrderByItem> orderByItems = order.getItems();
            for (SQLSelectOrderByItem orderByItem : orderByItems) {
                SQLExpr orderByExpr = orderByItem.getExpr();
                String on;
                if (orderByExpr instanceof SQLIdentifierExpr)
                    on = parseSQLIdentifierExpr((SQLIdentifierExpr) orderByExpr);
                else if (orderByExpr instanceof SQLPropertyExpr) {
                    String[] s = parseSQLPropertyExpr((SQLPropertyExpr) orderByExpr);
                    on = s[s.length - 1];
                    if (colMapper == null) {
                        TableStruct table = tables.get(0);
                        if (s.length == 2 && !table.equals(table.getSchema_catalog(), s[0]))
                            throw new SQLParseException("sql[" + this.sql + "] orderBy column[" + on + "] cant not find table");
                        else if (s.length == 3 && !table.equals(s[0], s[1]))
                            throw new SQLParseException("sql[" + this.sql + "] orderBy column[" + on + "] cant not find table");
                    }
                } else throw new NotSupportException(this.sql, "SQLSelectOrderByItem expr", orderByExpr.getClass());

                TableStruct table = null;
                if (colMapper != null) {
                    for (TableStruct _table : colMapper.keySet()) {
                        if (colMapper.get(_table).contains(on) || ("oracle".equals(wrapConnect.getDbType()) &&
                                colMapper.get(_table).contains(on.toUpperCase()))) {
                            table = _table;
                            break;
                        }
                    }
                } else table = tables.get(0);
                if (table == null) throw new SQLParseException("sql[" + this.sql + "] order column[" + on +
                        "] cant not find table");
                ColStruct colStruct = new ColStruct(table, on, "select");
                list.add(colStruct);
            }
        }
        return list;
    }

    private List<SQLBinaryOpExpr> filterRow(TableStruct table) throws NotSupportException, SQLException {
        List<Map<String, Object>> filters = DSGHandler.filterRow(wrapConnect.getAK(), user,
                table.getSchema_catalog(), table.getName());
        List<SQLBinaryOpExpr> list = new ArrayList<>(filters.size());
        for (Map<String, Object> filter : filters) {
            String colName = String.valueOf(filter.get("colname"));
            SQLPropertyExpr colExpr = new SQLPropertyExpr(new SQLPropertyExpr(
                    table.getSchema_catalog(), table.getName()), colName);
            SQLBinaryOperator operator;
            String action = String.valueOf(filter.get("action"));
            if ("=".equals(action)) operator = SQLBinaryOperator.Equality;
            else if (">".equals(action)) operator = SQLBinaryOperator.GreaterThan;
            else if (">=".equals(action)) operator = SQLBinaryOperator.GreaterThanOrEqual;
            else if ("<".equals(action)) operator = SQLBinaryOperator.LessThan;
            else if ("<=".equals(action)) operator = SQLBinaryOperator.LessThanOrEqual;
            else if ("!=".equals(action)) operator = SQLBinaryOperator.NotEqual;
            else throw new NotSupportException("row filter action[" + action + "] not support");
            String valType = String.valueOf(filter.getOrDefault("valtype", "varchar"))
                    .toLowerCase();
            String val = String.valueOf(filter.get("filterval"));
            SQLExpr valExpr;
            if ("varchar".equals(valType)) valExpr = new SQLCharExpr(val);
            else if ("number".equals(valType)) valExpr = new SQLNumberExpr(val.toCharArray());
            else throw new NotSupportException("row filter value type[" +
                        valType + "] not support");
            SQLBinaryOpExpr expr = new SQLBinaryOpExpr(colExpr, operator, valExpr,
                    wrapConnect.getDbType());
            list.add(expr);
        }
        return list;
    }


    private String parseSQLIdentifierExpr(SQLIdentifierExpr expr) {
        return expr.getName();
    }

    private ColStruct parseSQLPropertyExpr(SQLPropertyExpr expr, List<TableStruct> tables,
                                           Map<TableStruct, List<String>> colMapper, String operator)
            throws NotSupportException {
        List<TableStruct> _tables = new ArrayList<>(tables.size());
        _tables.addAll(tables);
        String[] s = parseSQLPropertyExpr(expr);
        String cn = s[s.length - 1];
        if (colMapper == null) {
            TableStruct table = _tables.get(0);
            if (s.length == 2 && !table.equals(table.getSchema_catalog(), s[0])) {
                TableStruct _table = new TableStruct(table.getSchema_catalog(), s[0]);
                _tables.set(0, _table);
            } else if (s.length == 3 && !table.equals(s[0], s[1])) {
                TableStruct _table = new TableStruct(s[0], s[1]);
                _tables.set(0, _table);
            }
        }
        TableStruct table = null;
        if (colMapper != null) {
            if (s.length == 2) {
                for (TableStruct _table : colMapper.keySet()) {
                    if (_table.equals(_table.getSchema_catalog(), s[0])) {
                        if (colMapper.get(_table).contains(cn) || ("oracle".equals(wrapConnect.getDbType()) &&
                                colMapper.get(_table).contains(cn.toUpperCase()))) {
                            table = _table;
                            break;
                        }
                    }
                }
            } else if (s.length == 3) {
                for (TableStruct _table : colMapper.keySet()) {
                    if (_table.equals(s[0], s[1])) {
                        if (colMapper.get(_table).contains(cn) || ("oracle".equals(wrapConnect.getDbType()) &&
                                colMapper.get(_table).contains(cn.toUpperCase()))) {
                            table = _table;
                            break;
                        }
                    }
                }
            }
//            for (TableStruct _table : colMapper.keySet()) {
//                if (s.length == 2 && _table.equals(_table.getSchema_catalog(), s[0])) {
//                    if (colMapper.get(_table).contains(cn) || ("oracle".equals(wrapConnect.getDbType()) &&
//                            colMapper.get(_table).contains(cn.toUpperCase()))) {
//                        table = _table;
//                        break;
//                    }
//                } else if (s.length == 3 && _table.equals(s[0], s[1])) {
//                    if (colMapper.get(_table).contains(cn) || ("oracle".equals(wrapConnect.getDbType()) &&
//                            colMapper.get(_table).contains(cn.toUpperCase()))) {
//                        table = _table;
//                        break;
//                    }
//                }
//            }
        } else table = _tables.get(0);
        if (table == null) throw new SQLParseException("sql[" + this.sql + "] column[" + cn +
                "] cant not find table");
        return new ColStruct(table, cn, operator);
    }

    private String[] parseSQLPropertyExpr(SQLPropertyExpr expr) throws NotSupportException {
        String name = expr.getName();
        SQLExpr owner = expr.getOwner();
        String[] s;
        if (owner instanceof SQLIdentifierExpr) {
            s = new String[2];
            s[0] = parseSQLIdentifierExpr((SQLIdentifierExpr) owner);
            s[1] = name;
        } else if (owner instanceof SQLPropertyExpr) {
            s = new String[3];
            String middle = ((SQLPropertyExpr) owner).getName();
            SQLExpr _owner = ((SQLPropertyExpr) owner).getOwner();
            if (_owner instanceof SQLIdentifierExpr) {
                s[0] = parseSQLIdentifierExpr((SQLIdentifierExpr) _owner);
                s[1] = middle;
                s[2] = name;
            } else
                throw new NotSupportException("sql[" + this.sql + "] SQLPropertyExpr level>3 not support");
        } else throw new NotSupportException(this.sql, "SQLPropertyExpr owner", owner.getClass());
        return s;
    }

    private List<String> parseSQLAllColumnExpr(TableStruct table) throws SQLException {
        try (PreparedStatement ps = wrapConnect.getDbConnect().prepareStatement(
                "select * from " + table.toString() + " where 1=0")) {
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int cols = rsmd.getColumnCount();
            List<String> list = new ArrayList<>(cols);
            for (int i = 1; i <= cols; i++) {
                list.add(rsmd.getColumnName(i));
            }
            rs.close();
            return list;
        }
    }

    private ColStruct parseColumn(String operator, TableStruct table, SQLExpr colExpr, String alias)
            throws NotSupportException, SQLException {
        if (colExpr instanceof SQLIdentifierExpr) {
            return new ColStruct(table,
                    parseSQLIdentifierExpr((SQLIdentifierExpr) colExpr), alias, operator);
        } else if (colExpr instanceof SQLPropertyExpr) {
            String[] s = parseSQLPropertyExpr((SQLPropertyExpr) colExpr);
            if (s.length == 2 && !table.equals(table.getSchema_catalog(), s[0]))
                throw new SQLParseException("sql[" + this.sql + "] parse error");
            if (s.length == 3 && !table.equals(s[0], s[1]))
                throw new SQLParseException("sql[" + this.sql + "] parse error");
            return new ColStruct(table, s[s.length - 1], alias, operator);
        } else if (colExpr instanceof SQLNumberExpr || colExpr instanceof SQLIntegerExpr
                || colExpr instanceof SQLCharExpr || colExpr instanceof SQLVariantRefExpr) {
            return null;
        } else if (colExpr instanceof SQLQueryExpr) {
            parseSQLSelect(((SQLQueryExpr) colExpr).getSubQuery());
            return null;
        } else if (colExpr instanceof SQLAggregateExpr) {
            SQLAggregateExpr aggregateExpr = (SQLAggregateExpr) colExpr;
            List<SQLExpr> arguments = aggregateExpr.getArguments();
            if (arguments != null) {
                if (arguments.size() > 1)
                    throw new NotSupportException(this.sql, aggregateExpr.getParent().toString());
                else return parseColumn(operator, table, arguments.get(0), null);
            } else return null;
        } else throw new NotSupportException(this.sql, "column expr", colExpr.getClass());
    }

    private TableStruct parseSQLExprTableSource(SQLExprTableSource tableSource) throws NotSupportException {
        String alias = tableSource.getAlias();
        String schema_catalog = tableSource.getSchema() == null
                ? wrapConnect.getSchema_catalog() : tableSource.getSchema();
        String tableName;
        SQLExpr expr = tableSource.getExpr();
        if (expr instanceof SQLIdentifierExpr) tableName = ((SQLIdentifierExpr) expr).getName();
        else if (expr instanceof SQLPropertyExpr) tableName = ((SQLPropertyExpr) expr).getName();
        else throw new NotSupportException(this.sql, "SQLExprTableSource expr", expr.getClass());
        return new TableStruct(schema_catalog, tableName, alias);
    }

    private ColStruct parseColumn(String operator, List<TableStruct> tables, SQLExpr colExpr, String alias,
                                  Map<TableStruct, List<String>> colMapper)
            throws NotSupportException, SQLException {
        String cn;
        ColStruct col = null;
        if (colExpr instanceof SQLIdentifierExpr) {
            cn = parseSQLIdentifierExpr((SQLIdentifierExpr) colExpr);
            for (TableStruct table : colMapper.keySet()) {
                List<String> originals = colMapper.get(table);
                if (originals.contains(cn) || ("oracle".equals(wrapConnect.getDbType()) &&
                        originals.contains(cn.toUpperCase()))) {
                    col = new ColStruct(table, cn, alias, operator);
                    break;
                }
            }
        } else if (colExpr instanceof SQLPropertyExpr) {
            String[] s = parseSQLPropertyExpr((SQLPropertyExpr) colExpr);
            String schema_catalog = s.length == 2 ? wrapConnect.getSchema_catalog() : s[0];
            String tableName = s.length == 2 ? s[0] : s[1];
            cn = s[s.length - 1];
            for (TableStruct table : tables) {
                if (table.equals(schema_catalog, tableName)) {
                    col = new ColStruct(table, cn, operator);
                    break;
                }
            }
        } else if (colExpr instanceof SQLNumberExpr || colExpr instanceof SQLIntegerExpr
                || colExpr instanceof SQLCharExpr || colExpr instanceof SQLVariantRefExpr) {
            return null;
        } else if (colExpr instanceof SQLQueryExpr) {
            parseSQLSelect(((SQLQueryExpr) colExpr).getSubQuery());
            return null;
        } else if (colExpr instanceof SQLAggregateExpr) {
            SQLAggregateExpr aggregateExpr = (SQLAggregateExpr) colExpr;
            List<SQLExpr> arguments = aggregateExpr.getArguments();
            if (arguments != null) {
                if (arguments.size() > 1)
                    throw new NotSupportException(this.sql, aggregateExpr.getParent().toString());
                else return parseColumn(operator, tables, arguments.get(0), null, colMapper);
            } else return null;
        } else throw new NotSupportException(this.sql, "column expr", colExpr.getClass());
        if (col == null) throw new SQLParseException("sql[" + this.sql +
                "] exist column[" + cn + "] can not find table");
        return col;
    }

    private void parseSQLJoinTableSource(SQLJoinTableSource tableSource, List<TableStruct> tables)
            throws NotSupportException {
        SQLTableSource left = tableSource.getLeft();
        SQLTableSource right = tableSource.getRight();

        if (left instanceof SQLExprTableSource) tables.add(parseSQLExprTableSource((SQLExprTableSource) left));
        else if (left instanceof SQLJoinTableSource) parseSQLJoinTableSource((SQLJoinTableSource) left, tables);
        else throw new NotSupportException("SQLJoinTableSource left[" + left.getClass() + "] not support");

        if (right instanceof SQLExprTableSource) tables.add(parseSQLExprTableSource((SQLExprTableSource) right));
        else if (right instanceof SQLJoinTableSource) parseSQLJoinTableSource((SQLJoinTableSource) right, tables);
        else throw new NotSupportException("SQLJoinTableSource right[" + right.getClass() + "] not support");
    }

    private void parseSQLWhere(SQLExpr expr, List<TableStruct> tables, Map<TableStruct, List<String>> colMapper)
            throws NotSupportException, SQLException {
        if (expr == null) return;
        if (expr instanceof SQLNumberExpr || expr instanceof SQLIntegerExpr || expr instanceof SQLCharExpr
                || expr instanceof SQLVariantRefExpr)
            return;
        assert tables.size() == 1 || colMapper != null;
        if (expr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) expr;
            SQLExpr left = sqlBinaryOpExpr.getLeft();
            if (left != null) parseSQLWhere(left, tables, colMapper);
            SQLExpr right = sqlBinaryOpExpr.getRight();
            if (right != null) parseSQLWhere(right, tables, colMapper);
        } else if (expr instanceof SQLIdentifierExpr) {
            String cn = parseSQLIdentifierExpr((SQLIdentifierExpr) expr);
            TableStruct table = null;
            if (colMapper != null) {
                for (TableStruct _table : colMapper.keySet()) {
                    if (colMapper.get(_table).contains(cn) || ("oracle".equals(wrapConnect.getDbType()) &&
                            colMapper.get(_table).contains(cn.toUpperCase()))) {
                        table = _table;
                        break;
                    }
                }
            } else table = tables.get(0);
            if (table == null) throw new SQLParseException("sql[" + this.sql + "] where column[" + cn +
                    "] cant not find table");
            ColStruct colStruct = new ColStruct(table, cn, "select");
            this.sqlStruct.addOther(colStruct);
        } else if (expr instanceof SQLBetweenExpr) {
            SQLBetweenExpr sqlBetweenExpr = (SQLBetweenExpr) expr;
            SQLExpr testExpr = sqlBetweenExpr.getTestExpr();
            if (testExpr != null) parseSQLWhere(testExpr, tables, colMapper);
            SQLExpr beginExpr = sqlBetweenExpr.getBeginExpr();
            if (beginExpr != null) parseSQLWhere(beginExpr, tables, colMapper);
            SQLExpr endExpr = sqlBetweenExpr.getEndExpr();
            if (endExpr != null) parseSQLWhere(endExpr, tables, colMapper);
        } else if (expr instanceof SQLInSubQueryExpr) {
            SQLInSubQueryExpr sqlInSubQueryExpr = (SQLInSubQueryExpr) expr;
            SQLExpr subExpr = sqlInSubQueryExpr.getExpr();
            if (subExpr != null) parseSQLWhere(subExpr, tables, colMapper);
            SQLSelect select = sqlInSubQueryExpr.getSubQuery();
            if (select != null) parseSQLSelect(select);
        } else if (expr instanceof SQLQueryExpr) {
            parseSQLSelect(((SQLQueryExpr) expr).getSubQuery());
        } else if (expr instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr methodInvokeExpr = (SQLMethodInvokeExpr) expr;
            List<SQLExpr> exprs = methodInvokeExpr.getParameters();
            if (exprs != null) for (SQLExpr sqlExpr : exprs) {
                parseSQLWhere(sqlExpr, tables, colMapper);
            }
        } else if (expr instanceof SQLPropertyExpr) {
            ColStruct col = parseSQLPropertyExpr((SQLPropertyExpr) expr, tables, colMapper, "select");
            this.sqlStruct.addOther(col);
        } else throw new NotSupportException(this.sql, "SQLWhere expr", expr.getClass());
    }
}
