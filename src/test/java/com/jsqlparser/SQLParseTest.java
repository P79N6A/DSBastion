package com.jsqlparser;

/*import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
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
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;*/

public class SQLParseTest {

   /* private SQLParse sqlParse;
    private Method copyMap;
    private Method parseSelect;
    private Method parseSubSelect;
    private Method parseSelectBody;
    private Method parseColumn;
    private Method parseWhere;
    private Method getClassName;

    @Before
    public void setUp() throws Exception {
        sqlParse = new SQLParse(null);
        copyMap = sqlParse.getClass().getDeclaredMethod("copyMap", Map.class, Map.class);
        copyMap.setAccessible(true);
        parseSelect = sqlParse.getClass().getDeclaredMethod("parseSelect", Select.class);
        parseSelect.setAccessible(true);
        parseSubSelect = sqlParse.getClass().getDeclaredMethod("parseSelect", SubSelect.class);
        parseSubSelect.setAccessible(true);
        parseSelectBody = sqlParse.getClass().getDeclaredMethod("parseSelectBody", SelectBody.class);
        parseSelectBody.setAccessible(true);
        parseColumn = sqlParse.getClass().getDeclaredMethod("parseColumn", Table.class, String.class, Column[].class);
        parseColumn.setAccessible(true);
        parseWhere = sqlParse.getClass().getDeclaredMethod("parseWhere", Expression.class, Table.class, Map.class);
        parseWhere.setAccessible(true);
        getClassName = sqlParse.getClass().getDeclaredMethod("getClassName", Class.class);
        getClassName.setAccessible(true);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testMergeMap() throws InvocationTargetException, IllegalAccessException {
        Map<String, Map<String, Object>> map = new HashMap<>();
        Map<String, Map<String, String>> m1 = new HashMap<>();
        Map<String, String> _m1 = new HashMap<>();
        _m1.put("1k", "1v");
        m1.put("1", _m1);
        copyMap.invoke(sqlParse, map, m1);
        Map<String, Map<String, Set<String>>> m2 = new HashMap<>();
        Map<String, Set<String>> _m2 = new HashMap<>();
        Set<String> s2 = new HashSet<>();
        s2.add("s1");
        s2.add("s2");
        _m2.put("2k", s2);
        m2.put("2", _m2);
        copyMap.invoke(sqlParse, map, m2);
        Map<String, Map<String, String>> m3 = new HashMap<>();
        Map<String, String> _m3 = new HashMap<>();
        _m3.put("3k", "3v");
        m3.put("1", _m3);
        copyMap.invoke(sqlParse, map, m3);
        System.out.println(map);
    }

    @Test
    public void testAlter() throws JSQLParserException, InvocationTargetException, IllegalAccessException {
        String sql = "ALTER TABLE Persons ADD Birthday date";
        sql = "ALTER TABLE Persons ALTER COLUMN Birthday year";
        sql = "ALTER TABLE Persons DROP COLUMN Birthday";
        Statement stmt = CCJSqlParserUtil.parse(new StringReader(sql));
        String operate = ((String) getClassName.invoke(sqlParse, stmt.getClass())).toLowerCase();
        assertEquals("alter", operate);
        Alter alter = (Alter) stmt;
        assertEquals("Persons", alter.getTable().getName());
    }

    @Test
    public void testCreateIndex() throws JSQLParserException, InvocationTargetException, IllegalAccessException {
        String sql = "CREATE INDEX PersonIndex ON Person (LastName DESC)";
        sql = "CREATE INDEX PersonIndex ON Person (LastName,FirstName)";
        sql = "CREATE UNIQUE INDEX PersonIndex ON Person (LastName,FirstName)";
        Statement stmt = CCJSqlParserUtil.parse(new StringReader(sql));
        String operate = ((String) getClassName.invoke(sqlParse, stmt.getClass())).toLowerCase();
        assertEquals("createindex", operate);
        CreateIndex createIndex = (CreateIndex) stmt;
        assertEquals("Person", createIndex.getTable().getName());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTable() throws JSQLParserException, InvocationTargetException, IllegalAccessException {
        String sql = "CREATE TABLE Persons (Id_P int,LastName varchar(255),FirstName varchar(255)," +
                "Address varchar(255),City varchar(255))";
        sql = "CREATE TABLE Persons AS (SELECT id, address, city, state, zip FROM companies WHERE id1> 1000)";
        sql = "CREATE TABLE Persons AS (SELECT id, address FROM (select id, address from companies WHERE id1 > 1000))";
        Statement stmt = CCJSqlParserUtil.parse(new StringReader(sql));
        String operate = ((String) getClassName.invoke(sqlParse, stmt.getClass())).toLowerCase();
        assertEquals("createtable", operate);
        CreateTable createTable = (CreateTable) stmt;
        Select select = createTable.getSelect();
        Map<String, Map<String, String>> map = (Map<String, Map<String, String>>) parseSelect.invoke(sqlParse, select);
        assertEquals("Persons", createTable.getTable().getName());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateView() throws JSQLParserException, InvocationTargetException, IllegalAccessException {
        String sql = "CREATE VIEW ProductsView AS SELECT ProductName,UnitPrice FROM Products " +
                "WHERE UnitPrice>(SELECT AVG(UnitPrice) FROM Products)";
        sql = "CREATE VIEW ProductsView AS SELECT ProductName,UnitPrice FROM Products " +
                "WHERE UnitPrice1>(SELECT AVG(UnitPrice) FROM Products1)";
        Statement stmt = CCJSqlParserUtil.parse(new StringReader(sql));
        String operate = ((String) getClassName.invoke(sqlParse, stmt.getClass())).toLowerCase();
        assertEquals("createview", operate);
        CreateView createView = (CreateView) stmt;
        Select select = createView.getSelect();
        Map<String, Map<String, String>> map = (Map<String, Map<String, String>>) parseSelect.invoke(sqlParse, select);
        assertEquals("ProductsView", createView.getView().getName());
    }

    @Test
    public void testDelete() throws JSQLParserException, InvocationTargetException, IllegalAccessException {
        String sql = "DELETE FROM Person WHERE LastName = 'Wilson' ";
        sql = "delete from Person where S_date not in " +
                "(select e2.maxdt from" +
                "(select Order_Id,Product_Id,Amt,MAX(S_date) as maxdt from Exam" +
                " group by Order_Id,Product_Id,Amt) as e2)";
        Statement stmt = CCJSqlParserUtil.parse(new StringReader(sql));
        String operate = ((String) getClassName.invoke(sqlParse, stmt.getClass())).toLowerCase();
        assertEquals("delete", operate);
        Delete delete = (Delete) stmt;
        assertEquals("Person", delete.getTable().getName());
        Map<String, Map<String, String>> map = new HashMap<>();
        Map<String, String> _m = new HashMap<>(1);
        _m.put(delete.getTable().getName(), operate);
        map.put(delete.getTable().getName(), _m);
        parseWhere.invoke(sqlParse, delete.getWhere(), delete.getTable(), map);
        assertEquals(2, map.size());
    }

    @Test
    public void testDrop() throws JSQLParserException, InvocationTargetException, IllegalAccessException {
        String sql = "DROP TABLE Customer;";
        Statement stmt = CCJSqlParserUtil.parse(new StringReader(sql));
        String operate = ((String) getClassName.invoke(sqlParse, stmt.getClass())).toLowerCase();
        assertEquals("drop", operate);
        Drop drop = (Drop) stmt;
        assertEquals("Customer", drop.getName().getName());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testInsert() throws JSQLParserException, InvocationTargetException, IllegalAccessException {
        String sql = "INSERT INTO Store_Information (Store_Name, Sales, Txn_Date) VALUES ('Los Angeles', 900, 'Jan-10-1999')";
        sql = "INSERT INTO Store_Information (Store_Name, Sales, Txn_Date) " +
                "SELECT store_name, Sales, Txn_Date FROM Sales_Information " +
                "WHERE Year(Txn_Date1) = 1998";
        Statement stmt = CCJSqlParserUtil.parse(new StringReader(sql));
        String operate = ((String) getClassName.invoke(sqlParse, stmt.getClass())).toLowerCase();
        assertEquals("insert", operate);
        Insert insert = (Insert) stmt;
        assertEquals("Store_Information", insert.getTable().getName());
        if (insert.getSelect() != null) {
            Map<String, Map<String, String>> map = (Map<String, Map<String, String>>) parseSelect.invoke(sqlParse, insert.getSelect());
            assertEquals(1, map.size());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSelect() throws JSQLParserException, InvocationTargetException, IllegalAccessException {
        String sql = "SELECT id,name,time FROM table1 WHERE id2 IN (SELECT id3 FROM table2 WHERE name2 like 'z%')";
        Statement stmt = CCJSqlParserUtil.parse(new StringReader(sql));
        String operate = ((String) getClassName.invoke(sqlParse, stmt.getClass())).toLowerCase();
        assertEquals("select", operate);
        Select select = (Select) stmt;
        Map<String, Map<String, String>> map = null;
        if (select.getSelectBody() != null)
            map = (Map<String, Map<String, String>>) parseSelect.invoke(sqlParse, select);
        assertNotNull(map);
    }

    @Test
    public void testTruncate() throws JSQLParserException, InvocationTargetException, IllegalAccessException {
        String sql = "TRUNCATE TABLE Customer";
        Statement stmt = CCJSqlParserUtil.parse(new StringReader(sql));
        String operate = ((String) getClassName.invoke(sqlParse, stmt.getClass())).toLowerCase();
        assertEquals("truncate", operate);
        Truncate truncate = (Truncate) stmt;
        assertEquals("Customer", truncate.getTable().getName());
    }

    @Test
    public void testUpdate() throws JSQLParserException, InvocationTargetException, IllegalAccessException {
        String sql = "UPDATE Store_Information SET Sales = 500 WHERE Store_Name = 'Los Angeles' " +
                "AND Txn_Date = 'Jan-08-1999';";
        sql = "update Store_Information set shop_money=(select shop_money from build_info2 where build_info2.id=Store_Information.id)" +
                "where Store_Information.user = build_info2.user and Store_Information.user = 'test3' and shop_money =0";
        Statement stmt = CCJSqlParserUtil.parse(new StringReader(sql));
        String operate = ((String) getClassName.invoke(sqlParse, stmt.getClass())).toLowerCase();
        assertEquals("update", operate);
        Update update = (Update) stmt;
        Table tb = update.getTables().get(0);
        Map<String, Map<String, String>> map = new HashMap<>();
        assertEquals("Store_Information", tb.getName());
        Map<String, String> _m = new HashMap<>();
        _m.put(tb.getName(), operate);
        map.put(tb.getName(), _m);
        List<Column> columns = update.getColumns();
        if (columns != null) for (Column column : columns) map.get(tb.getName()).put(column.getColumnName(), operate);
        if (update.getWhere() != null) parseWhere.invoke(sqlParse, update.getWhere(), tb, map);
        if (update.getSelect() != null)
            copyMap.invoke(sqlParse, map, parseSelect.invoke(sqlParse, update.getSelect()));
        List<Expression> expressions = update.getExpressions();
        if (expressions != null) for (Expression exp : expressions) {
            if (exp instanceof SubSelect)
                copyMap.invoke(sqlParse, map, parseSubSelect.invoke(sqlParse, (SubSelect) exp));
        }
        assertEquals(1, update.getColumns().size());
    }*/
}