package com.handler;

import com.bean.*;
import com.util.InnerDb;
import org.junit.*;


import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.List;

import static org.junit.Assert.*;

public class SQLHandlerTest {

    private WrapConnect wrapConnect;

    @Before
    public void setUp() throws Exception {
        System.setProperty("JPPath", System.getProperty("user.dir"));
//        wrapConnect = new WrapConnect("/127.0.0.1:4257", "3074559825718491745");  //oracle
        wrapConnect = new WrapConnect("/127.0.0.1:4257", "-777050302049221812");  //mysql
    }

    @After
    public void tearDown() throws Exception {
        if (wrapConnect != null) wrapConnect.close();
    }

    @Test
    public void testAlter() throws Exception {
        String sql = "ALTER TABLE Persons ADD Birthday date";
//        sql = "ALTER TABLE db.Persons DROP COLUMN Birthday";
        SQLHandler sqlHandler = new SQLHandler(wrapConnect, "test", sql);
        sqlHandler.handler();
        SQLStruct sqlStruct = sqlHandler.getSqlStruct();
        assertEquals(1, sqlStruct.getFirst().size());
    }

    @Test
    public void testCreateIndex() {
        String sql = "CREATE INDEX PersonIndex ON Person (LastName DESC)";
        sql = "CREATE INDEX PersonIndex ON Person (LastName,FirstName)";
        sql = "CREATE UNIQUE INDEX PersonIndex ON db.Person (LastName,FirstName)";
    }

    @Test
    public void testCreateTable() throws Exception {
        String sql = "CREATE TABLE Persons (Id_P int,LastName varchar(255),FirstName varchar(255)," +
                "Address varchar(255),City varchar(255))";
        sql = "CREATE TABLE Persons AS SELECT id, address, city, state, zip FROM companies WHERE id1> 1000";
        sql = "CREATE TABLE Persons AS (SELECT id, address, city, state, zip FROM companies WHERE id1> 1000)";
        sql = "CREATE TABLE Persons AS (SELECT id, address FROM (select id, name, address from db.companies WHERE id1 > 1000))";
        SQLHandler sqlHandler = new SQLHandler(wrapConnect, "test", sql);
        sqlHandler.handler();
        SQLStruct sqlStruct = sqlHandler.getSqlStruct();
        assertEquals(1, sqlStruct.getFirst().size());
    }

    @Test
    public void testCreateView() {
        String sql = "CREATE VIEW ProductsView AS SELECT ProductName,UnitPrice FROM Products " +
                "WHERE UnitPrice>(SELECT AVG(UnitPrice) FROM Products)";
        sql = "CREATE VIEW ProductsView AS SELECT ProductName,UnitPrice FROM Products " +
                "WHERE UnitPrice1>(SELECT AVG(UnitPrice) FROM Products1)";
    }

    @Test
    public void testDelete() throws Exception {
        String sql = "DELETE FROM Person WHERE LastName = 'Wilson' ";
        sql = "delete from Person where S_date not in " +
                "(select e2.maxdt from" +
                "(select Order_Id as oid,Product_Id,Amt,MAX(S_date) as maxdt from Exam" +
                " group by Order_Id,Product_Id,Amt) as e2)";
        SQLHandler sqlHandler = new SQLHandler(wrapConnect, "test", sql);
        sqlHandler.handler();
        SQLStruct sqlStruct = sqlHandler.getSqlStruct();
        assertEquals(1, sqlStruct.getFirst().size());
    }

    @Test
    public void testDrop() throws Exception {
        String sql = "DROP TABLE Customer";
        sql = "DROP TABLE Customer,d2.producer";
        SQLHandler sqlHandler = new SQLHandler(wrapConnect, "test", sql);
        sqlHandler.handler();
        SQLStruct sqlStruct = sqlHandler.getSqlStruct();
        assertEquals(2, sqlStruct.getFirst().size());
    }

    @Test
    public void testInsert() throws Exception {
        String sql = "INSERT INTO Store_Information (Store_Name, Sales, Txn_Date) VALUES ('Los Angeles', 900, 'Jan-10-1999')";
        sql = "INSERT INTO Store_Information (Store_Name, Sales, Txn_Date) " +
                "SELECT store_name, sales, txn_Date FROM Sales_Information " +
                "WHERE Year(Txn_Date1) = 1998";
        SQLHandler sqlHandler = new SQLHandler(wrapConnect, "test", sql);
        sqlHandler.handler();
        SQLStruct sqlStruct = sqlHandler.getSqlStruct();
        assertEquals(3, sqlStruct.getFirst().size());
    }

    @Test
    public void testTruncate() throws Exception {
        String sql = "TRUNCATE TABLE Customer";
        SQLHandler sqlHandler = new SQLHandler(wrapConnect, "test", sql);
        sqlHandler.handler();
        SQLStruct sqlStruct = sqlHandler.getSqlStruct();
        assertEquals(1, sqlStruct.getFirst().size());
    }

    @Test
    public void testUpdate() throws Exception {
        String sql = "UPDATE Store_Information SET Sales = 500 WHERE Store_Name = 'Los Angeles' " +
                "AND Txn_Date = 'Jan-08-1999';";
        sql = "update Store_Information set shop_money=(select shop_money from build_info2 where build_info2.id=Store_Information.id)" +
                " where Store_Information.user = build_info2.user and Store_Information.user = 'test3'";
        SQLHandler sqlHandler = new SQLHandler(wrapConnect, "test", sql);
        sqlHandler.handler();
        SQLStruct sqlStruct = sqlHandler.getSqlStruct();
        assertEquals(1, sqlStruct.getFirst().size());
    }

    @Test
    public void testSelect() throws Exception {
        String sql = "select id from test where col1 like '北京' and date > '2019-02-28T09:43:10.224000'";
        sql = "select id,name from test where (col3='test' and col1 like 'a?b') or date>'2019-02-28T09:43:10.224000'";
        sql = "select id,name from test where (col3='test' and col1 like 'a?b') or " +
                "(col2>3 or date>'2019-02-28T09:43:10.224000') and col4='北京'";
        sql = "SELECT id,name,time FROM table1 WHERE id2 IN (SELECT id3 FROM table2 WHERE name2 like 'z%')";
        sql = "select applications.applicantId, lid, l.aid, h.hid, zjhm, xm, hname, roomId, r.rid, l.rent " +
                "from leases l, applications, applicants, houses h, rooms r " +
                "where l.hid = h.hid and l.rid = r.rid and l.aid = applications.aid and " +
                "applications.applicantId = applicants.applicantId";
        SQLHandler sqlHandler = new SQLHandler(wrapConnect, "test", sql);
        sqlHandler.handler();
        SQLStruct sqlStruct = sqlHandler.getSqlStruct();
        assertEquals(10, sqlStruct.getFirst().size());
    }

    @Test
    public void encryptValue() throws Exception {
        String sql = "INSERT INTO test VALUES (1, 'Los Angeles'),(2, 'Smith')";
        sql = "UPDATE test SET name = 'Lily' WHERE id = 1";
        SQLHandler sqlHandler = new SQLHandler(wrapConnect, "test", sql);
        sqlHandler.handler();
//        InnerDb.insert(wrapConnect.getDbConnect(), sqlHandler.getSql());
        InnerDb.update(wrapConnect.getDbConnect(), sqlHandler.getSql());
    }

    @Test
    public void decryptValue() throws Exception {
        String sql = "select * from test";
        SQLHandler sqlHandler = new SQLHandler(wrapConnect, "test", sql);
        sqlHandler.handler();
        List<ColStruct> selects = sqlHandler.getSqlStruct().getFirst();
        Statement stmt = wrapConnect.getDbConnect().createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            for (int i = 0; i < selects.size(); i++) {
                ColStruct col = selects.get(i);
                if ("name".equals(col.getName()) || ("oracle".equals(wrapConnect.getDbType()) &&
                        "name".equals(col.getName().toLowerCase()))) {
                    byte[] decrypt = CipherHandler.decrypt(rs.getString(i + 1).getBytes(StandardCharsets.UTF_8),
                            col.getMaskBean());
                    System.out.println(col + "[" + new String(decrypt, StandardCharsets.UTF_8) + "]");
                } else System.out.println(col + "[" + rs.getObject(i + 1) + "]");
            }
        }
        rs.close();
    }

}