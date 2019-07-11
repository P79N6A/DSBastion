package com.jsqlparser;

/*import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NamedExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;*/

/**
 * 对SQL语句拆解取出其中表,列信息及对应的操作
 */
public class SQLParse {

   /* private Connection connect;

    public SQLParse(Connection connect) {
        this.connect = connect;
    }

    *//**
     * <tableName,<tableName|columnName,operation|[operations]>>
     *//*
    public Map<String, Map<String, Object>> getOperation(String sql) throws JSQLParserException {
        Statement stmt = CCJSqlParserUtil.parse(new StringReader(sql));
        Map<String, Map<String, Object>> map = new HashMap<>();
        String operate = getClassName(stmt.getClass()).toLowerCase();
        if (stmt instanceof Alter) {
            Alter alter = (Alter) stmt;
            String tb = alter.getTable().getName();
            map.put(tb, Collections.singletonMap(tb, operate));
        } else if (stmt instanceof CreateIndex) {
            CreateIndex createIndex = (CreateIndex) stmt;
            String tb = createIndex.getTable().getName();
            map.put(tb, Collections.singletonMap(tb, operate));
        } else if (stmt instanceof CreateTable) {
            CreateTable createTable = (CreateTable) stmt;
            String tb = createTable.getTable().getName();
            Map<String, Object> _m = new HashMap<>(1);
            _m.put(tb, operate);
            map.put(tb, _m);
            if (createTable.getSelect() != null) copyMap(map, parseSelect(createTable.getSelect()));
        } else if (stmt instanceof CreateView) {
            CreateView createView = (CreateView) stmt;
            String tb = createView.getView().getName();
            Map<String, Object> _m = new HashMap<>(1);
            _m.put(tb, operate);
            map.put(tb, _m);
            if (createView.getSelect() != null) copyMap(map, parseSelect(createView.getSelect()));
        } else if (stmt instanceof Delete) {
            Delete delete = (Delete) stmt;
            Table tb = delete.getTable();
            Map<String, Object> _m = new HashMap<>(1);
            _m.put(tb.getName(), operate);
            map.put(tb.getName(), _m);
            if (delete.getWhere() != null) parseWhere(delete.getWhere(), tb, map);
        } else if (stmt instanceof Drop) {
            Drop drop = (Drop) stmt;
            String tb = drop.getName().getName();
            map.put(tb, Collections.singletonMap(tb, operate));
        } else if (stmt instanceof Insert) {
            Insert insert = (Insert) stmt;
            String tb = insert.getTable().getName();
            Map<String, Object> _m = new HashMap<>(1);
            _m.put(tb, operate);
            map.put(tb, _m);
            if (insert.getSelect() != null) copyMap(map, parseSelect(insert.getSelect()));
        } else if (stmt instanceof Select) {
            Select select = (Select) stmt;
            if (select.getSelectBody() != null) map = parseSelect(select);
            if (select.getWithItemsList() != null) throw new JSQLParserException("select...with is not support");
        } else if (stmt instanceof Truncate) {
            Truncate truncate = (Truncate) stmt;
            String tb = truncate.getTable().getName();
            map.put(tb, Collections.singletonMap(tb, operate));
        } else if (stmt instanceof Update) {
            Update update = (Update) stmt;
            if (update.getTables().size() > 1) throw new JSQLParserException("update table size>1 is not support");
            Table tb = update.getTables().get(0);
            Map<String, Object> _m = new HashMap<>();
            _m.put(tb.getName(), operate);
            map.put(tb.getName(), _m);
            List<Column> columns = update.getColumns();
            if (columns != null) {
                Column[] cols = new Column[columns.size()];
                copyMap(map, parseColumn(tb, operate, columns.toArray(cols)));
            }
            if (update.getWhere() != null) parseWhere(update.getWhere(), tb, map);
            if (update.getSelect() != null) copyMap(map, parseSelect(update.getSelect()));
            List<Expression> expressions = update.getExpressions();
            if (expressions != null) for (Expression exp : expressions) {
                if (exp instanceof SubSelect) copyMap(map, parseSelect((SubSelect) exp));
            }
        } else {
            throw new JSQLParserException("operation [" + operate + "] is not support");
        }
        return map;
    }


    private Map<String, Map<String, Object>> parseSelect(Select select) throws JSQLParserException {
        if (select == null) return Collections.emptyMap();
        SelectBody selectBody = select.getSelectBody();
        List<WithItem> withs = select.getWithItemsList();
        if (withs != null && withs.size() > 0) throw new JSQLParserException("select...with解析暂未支持");
        if (selectBody != null) return parseSelectBody(selectBody);
        return Collections.emptyMap();
    }

    private Map<String, Map<String, Object>> parseSelect(SubSelect select) throws JSQLParserException {
        if (select == null) return Collections.emptyMap();
        SelectBody selectBody = select.getSelectBody();
        List<WithItem> withs = select.getWithItemsList();
        if (withs != null && withs.size() > 0) throw new JSQLParserException("select...with解析暂未支持");
        if (selectBody != null) return parseSelectBody(selectBody);
        return Collections.emptyMap();
    }

    private void parseWhere(Expression expression, Table tb, Map<String, Map<String, Object>> map)
            throws JSQLParserException {
        if (expression == null) return;
        Class clazz = expression.getClass();
        if (Parenthesis.class.equals(clazz)) {
            Parenthesis parenthesis = (Parenthesis) expression;
            Expression pe = parenthesis.getExpression();
            parseWhere(pe, tb, map);
        } else if (AndExpression.class.equals(clazz)) {
            AndExpression and = (AndExpression) expression;
            parseWhere(and.getLeftExpression(), tb, map);
            parseWhere(and.getRightExpression(), tb, map);
        } else if (OrExpression.class.equals(clazz)) {
            OrExpression or = (OrExpression) expression;
            parseWhere(or.getLeftExpression(), tb, map);
            parseWhere(or.getRightExpression(), tb, map);
        } else {
            Expression left;
            Expression right;
            if (EqualsTo.class.equals(clazz)) {
                left = ((EqualsTo) expression).getLeftExpression();
                right = ((EqualsTo) expression).getRightExpression();
            } else if (GreaterThan.class.equals(clazz)) {
                left = ((GreaterThan) expression).getLeftExpression();
                right = ((GreaterThan) expression).getRightExpression();
            } else if (GreaterThanEquals.class.equals(clazz)) {
                left = ((GreaterThanEquals) expression).getLeftExpression();
                right = ((GreaterThanEquals) expression).getRightExpression();
            } else if (MinorThan.class.equals(clazz)) {
                left = ((MinorThan) expression).getLeftExpression();
                right = ((MinorThan) expression).getRightExpression();
            } else if (MinorThanEquals.class.equals(clazz)) {
                left = ((MinorThanEquals) expression).getLeftExpression();
                right = ((MinorThanEquals) expression).getRightExpression();
            } else if (LikeExpression.class.equals(clazz)) {
                left = ((LikeExpression) expression).getLeftExpression();
                right = ((LikeExpression) expression).getRightExpression();
            } else if (Between.class.equals(clazz)) {
                left = ((Between) expression).getLeftExpression();
                right = null;
            } else if (InExpression.class.equals(clazz)) {
                left = ((InExpression) expression).getLeftExpression();
                ItemsList rl = ((InExpression) expression).getRightItemsList();
                if (rl instanceof SubSelect) right = (SubSelect) rl;
                else throw new JSQLParserException("in condition[" + rl.getClass() + "] is not support");
            } else throw new JSQLParserException("where condition[" + expression.getClass() + "] is not support");

            if (left instanceof Column) copyMap(map, parseColumn(tb, "select", (Column) left));
            else if (left instanceof Function) copyMap(map, parseFunction(left, tb));
            else throw new JSQLParserException("where left expression[" + left.getClass() + "] is not support");

            if (right instanceof SubSelect) copyMap(map, parseSelect((SubSelect) right));
            else if (right instanceof Column) copyMap(map, parseColumn(tb, "select", (Column) right));
        }
    }

    private Map<String, Map<String, Object>> parseSelectBody(SelectBody selectBody)
            throws JSQLParserException {
        if (selectBody == null) return Collections.emptyMap();
        Map<String, Map<String, Object>> map = new HashMap<>();
        if (selectBody instanceof PlainSelect) {
            PlainSelect plainSelect = (PlainSelect) selectBody;
            FromItem fromItem = plainSelect.getFromItem();
            List<SelectItem> selectItems = plainSelect.getSelectItems();
            Expression where = plainSelect.getWhere();
            List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
            Table table = null;

            if (fromItem instanceof SubSelect) {
                copyMap(map, parseSelect((SubSelect) fromItem));
            } else if (fromItem instanceof Table) {
                table = (Table) fromItem;
                String tableName = table.getName();
                Map<String, Object> rm = new HashMap<>();
                rm.put(tableName, "select");
                map.put(tableName, rm);
            } else throw new JSQLParserException("select...from[" + fromItem.getClass() + "]解析暂未支持");
            if (table != null && selectItems != null) {
                for (SelectItem selectItem : selectItems) {
                    if (selectItem instanceof AllColumns || selectItem instanceof AllTableColumns) {
                        List<String> cols = getAllCol(table.getName());
                        for (String col : cols) {
                            map.get(table.getName()).put(col, "select");
                        }
                    } else {
                        Expression expression = ((SelectExpressionItem) selectItem).getExpression();
                        if (expression instanceof Column)
                            copyMap(map, parseColumn(table, "select", (Column) expression));
                        else if (expression instanceof Function) copyMap(map, parseFunction(expression, table));
                        else throw new JSQLParserException("SelectExpressionItem[" + expression.getClass() +
                                    "] is not support");
                    }
                }
            }
            if (table != null && where != null) parseWhere(where, table, map);
            if (table != null && orderByElements != null) {
                for (OrderByElement order : orderByElements) {
                    Expression expression = order.getExpression();
                    if (expression instanceof Column) {
                        copyMap(map, parseColumn(table, "select", (Column) expression));
                    } else
                        throw new JSQLParserException("order expression[" + expression.getClass() + "] is not support");
                }
            }
        } else throw new JSQLParserException("select body[" + selectBody.getClass() + "]解析暂未支持");
        return map;
    }

    private List<String> getAllCol(String tableName) throws JSQLParserException {
        try (PreparedStatement ps = connect.prepareStatement("select * from " + tableName + " where 1=0")) {
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int cols = rsmd.getColumnCount();
            List<String> list = new ArrayList<>(cols);
            for (int i = 1; i <= cols; i++) {
                list.add(rsmd.getColumnName(i));
            }
            rs.close();
            return list;
        } catch (SQLException e) {
            throw new JSQLParserException(e.getMessage(), e);
        }
    }

    private Map<String, Map<String, Object>> parseColumn(Table tb, String operate, Column... cols) {
        if (cols == null) return Collections.emptyMap();
        Map<String, Map<String, Object>> map = new HashMap<>(1);
        for (Column col : cols) {
            Table t = col.getTable();
            if (t == null) put(map, tb.getName(), col.getColumnName(), operate);
            else {
                String tn = t.getName();
                String alias = null;
                if (tb.getAlias() != null) alias = tb.getAlias().getName();
                if (tn.equals(tb.getName()) || tn.equals(alias)) put(map, tb.getName(), col.getColumnName(), operate);
                else put(map, tn, col.getColumnName(), operate);
            }
        }
        return map;
    }

    private void put(Map<String, Map<String, Object>> map, String key, String kk, Object vv) {
        if (map.containsKey(key)) map.get(key).put(kk, vv);
        else {
            Map<String, Object> _map = new HashMap<>(1);
            _map.put(kk, vv);
            map.put(key, _map);
        }
    }

    private Map<String, Map<String, Object>> parseFunction(Expression expression, Table tb)
            throws JSQLParserException {
        if (expression == null) return Collections.emptyMap();
        Map<String, Map<String, Object>> map = new HashMap<>(1);
        Function function = (Function) expression;
        NamedExpressionList nl = function.getNamedParameters();
        if (nl != null) throw new JSQLParserException("function[" + nl.getClass() + "] is not support");
        ExpressionList el = function.getParameters();
        if (el != null) for (Expression ele : el.getExpressions()) {
            if (ele instanceof Column) {
                copyMap(map, parseColumn(tb, "select", (Column) ele));
            } else throw new JSQLParserException("function expression[" + ele.getClass() +
                    "] is not support");
        }
        return map;
    }

    *//**
     * one table may be has multi operation
     * last object can be string or set
     *//*
    @SuppressWarnings("unchecked")
    private void copyMap(Map<String, Map<String, Object>> src,
                         Map<String, Map<String, Object>> dst) {
        for (String dk : dst.keySet()) {
            if (src.containsKey(dk)) {
                Map<String, Object> dpm = dst.get(dk);
                Map<String, Object> spm = src.get(dk);
                for (String dpk : dpm.keySet()) {
                    Object dv = dpm.get(dpk);
                    if (spm.containsKey(dpk)) {
                        Object sv = spm.get(dpk);
                        if (sv instanceof String) {
                            if (dv instanceof String) {
                                String _dv = (String) dv;
                                String _sv = (String) sv;
                                if (!_dv.equals(_sv)) {
                                    Set<String> set = new HashSet<>(2);
                                    set.add(_dv);
                                    set.add(_sv);
                                    spm.put(dpk, set);
                                }
                            } else {
                                Set<String> dvs = (Set<String>) dv;
                                dvs.add((String) sv);
                                spm.put(dpk, dvs);
                            }
                        } else {
                            Set<String> svs = (Set<String>) sv;
                            if (dv instanceof String) svs.add((String) dv);
                            else {
                                Set<String> dvs = (Set<String>) dv;
                                svs.addAll(dvs);
                            }
                        }
                    } else spm.put(dpk, dv);
                }
            } else src.put(dk, dst.get(dk));
        }
        dst.clear();
    }


    private String getClassName(Class clazz) {
        String fullName = clazz.getName();
        return fullName.substring(fullName.lastIndexOf(".") + 1);
    }*/
}
