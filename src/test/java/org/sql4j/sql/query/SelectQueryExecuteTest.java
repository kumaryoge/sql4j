package org.sql4j.sql.query;

import lombok.Builder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.List;

import static org.sql4j.sql.query.Column.*;
import static org.sql4j.sql.query.Table.TABLE_1;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SelectQueryExecuteTest {
    private static Connection CON;

    @BeforeAll
    public static void setUp() throws ClassNotFoundException, SQLException {
        // Load the H2 driver
        Class.forName("org.h2.Driver");

        // Connect to the in-memory database
        String url = "jdbc:h2:mem:testdb"; // testdb is the database name
        String user = "sa";
        String password = "";
        CON = DriverManager.getConnection(url, user, password);
        System.out.println("Connected to H2 database!");

        try (Statement stmt = CON.createStatement()) {
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS TABLE_1 (
                        COL_1 VARCHAR(255) PRIMARY KEY,
                        COL_2 INT NOT NULL
                    )""");
            System.out.println("TABLE_1 created.");
        }
    }

    @AfterAll
    public static void tearDown() throws SQLException {
        try (Statement stmt = CON.createStatement()) {
            stmt.executeUpdate("DROP TABLE IF EXISTS TABLE_1");
            System.out.println("TABLE_1 dropped.");
        }
        // Close the connection
        CON.close();
    }

    @Test
    void testSelectQuery_singleColumn_singleTable() throws SQLException {
        insertRecordInTable1("test", 1);

        List<String> results =
                SqlQuery.select(COL_1)
                        .from(TABLE_1)
                        .execute(CON, rs -> rs.getString(COL_1.getName()));
        assertEquals(1, results.size());
        assertEquals("test", results.getFirst());

        deleteRecordFromTable1("test", 1);
    }

    @Test
    void testSelectQuery_multipleColumns_singleTable() throws SQLException {
        insertRecordInTable1("test", 1);

        List<Table1Row> results =
                SqlQuery.select(COL_1, COL_2)
                        .from(TABLE_1)
                        .execute(CON, rs -> Table1Row.builder()
                                .col1(rs.getString(COL_1.getName()))
                                .col2(rs.getInt(COL_2.getName()))
                                .build());
        assertEquals(1, results.size());
        assertEquals("test", results.getFirst().col1());
        assertEquals(1, results.getFirst().col2());

        deleteRecordFromTable1("test", 1);
    }

    @Test
    void testSelectQuery_multipleColumns_singleTable_multipleRecords() throws SQLException {
        insertRecordInTable1("test1", 1);
        insertRecordInTable1("test2", 2);

        List<Table1Row> results =
                SqlQuery.select(COL_1, COL_2)
                        .from(TABLE_1)
                        .execute(CON, rs -> Table1Row.builder()
                                .col1(rs.getString(COL_1.getName()))
                                .col2(rs.getInt(COL_2.getName()))
                                .build());
        assertEquals(2, results.size());
        assertEquals("test1", results.getFirst().col1());
        assertEquals(1, results.getFirst().col2());
        assertEquals("test2", results.getLast().col1());
        assertEquals(2, results.getLast().col2());

        deleteRecordFromTable1("test1", 1);
        deleteRecordFromTable1("test2", 2);
    }

    @Test
    void testSelectQuery_multipleColumns_singleTable_singleCondition() throws SQLException {
        insertRecordInTable1("test1", 1);
        insertRecordInTable1("test2", 2);

        List<Table1Row> results =
                SqlQuery.select(COL_1, COL_2)
                        .from(TABLE_1)
                        .where(COL_1.equalTo("test1"))
                        .execute(CON, rs -> Table1Row.builder()
                                .col1(rs.getString(COL_1.getName()))
                                .col2(rs.getInt(COL_2.getName()))
                                .build());
        assertEquals(1, results.size());
        assertEquals("test1", results.getFirst().col1());
        assertEquals(1, results.getFirst().col2());

        deleteRecordFromTable1("test1", 1);
        deleteRecordFromTable1("test2", 2);
    }

    @Test
    void testSelectQuery_allColumns_singleTable() throws SQLException {
        insertRecordInTable1("test", 1);

        List<Table1Row> results =
                SqlQuery.select(ALL)
                        .from(TABLE_1)
                        .execute(CON, rs -> Table1Row.builder()
                                .col1(rs.getString(COL_1.getName()))
                                .col2(rs.getInt(COL_2.getName()))
                                .build());
        assertEquals(1, results.size());
        assertEquals("test", results.getFirst().col1());
        assertEquals(1, results.getFirst().col2());

        deleteRecordFromTable1("test", 1);
    }

    @Test
    void testSelectQuery_allColumns_singleTable_count() throws SQLException {
        insertRecordInTable1("test1", 1);
        insertRecordInTable1("test2", 2);

        List<Integer> results =
                SqlQuery.select(ALL.count())
                        .from(TABLE_1)
                        .execute(CON, rs -> rs.getInt(ALL.count().getName()));
        assertEquals(1, results.size());
        assertEquals(2, results.getFirst());

        deleteRecordFromTable1("test1", 1);
        deleteRecordFromTable1("test2", 2);
    }

    @Test
    void testSelectQuery_singleColumn_singleTable_count() throws SQLException {
        insertRecordInTable1("test1", 1);
        insertRecordInTable1("test2", 2);

        List<Integer> results =
                SqlQuery.select(COL_1.count())
                        .from(TABLE_1)
                        .execute(CON, rs -> rs.getInt(COL_1.count().getName()));
        assertEquals(1, results.size());
        assertEquals(2, results.getFirst());

        deleteRecordFromTable1("test1", 1);
        deleteRecordFromTable1("test2", 2);
    }

    @Test
    void testSelectQuery_singleColumn_singleTable_distinct() throws SQLException {
        insertRecordInTable1("test1", 10);
        insertRecordInTable1("test2", 10);

        List<String> results =
                SqlQuery.select(COL_1.distinct())
                        .from(TABLE_1)
                        .execute(CON, rs -> rs.getString(COL_1.getName()));
        assertEquals(2, results.size());
        assertEquals("test1", results.getFirst());
        assertEquals("test2", results.getLast());

        deleteRecordFromTable1("test1", 10);
        deleteRecordFromTable1("test2", 10);
    }

    @Test
    void testSelectQuery_singleColumn_singleTable_distinct2() throws SQLException {
        insertRecordInTable1("test1", 10);
        insertRecordInTable1("test2", 10);

        List<Integer> results =
                SqlQuery.select(COL_2.distinct())
                        .from(TABLE_1)
                        .execute(CON, rs -> rs.getInt(COL_2.getName()));
        assertEquals(1, results.size());
        assertEquals(10, results.getFirst());

        deleteRecordFromTable1("test1", 10);
        deleteRecordFromTable1("test2", 10);
    }

    @Test
    void testSelectQuery_singleColumn_singleTable_countDistinct() throws SQLException {
        insertRecordInTable1("test1", 10);
        insertRecordInTable1("test2", 10);

        List<Integer> results =
                SqlQuery.select(COL_2.countDistinct())
                        .from(TABLE_1)
                        .execute(CON, rs -> rs.getInt(COL_2.countDistinct().getName()));
        assertEquals(1, results.size());
        assertEquals(1, results.getFirst());

        deleteRecordFromTable1("test1", 10);
        deleteRecordFromTable1("test2", 10);
    }

    private void insertRecordInTable1(String col1, int col2) throws SQLException {
        try (PreparedStatement stmt = CON.prepareStatement("INSERT INTO TABLE_1 (COL_1, COL_2) VALUES (?, ?)")) {
            stmt.setString(1, col1);
            stmt.setInt(2, col2);
            int numRowsInserted = stmt.executeUpdate();
            System.out.printf("Record (%s, %s) inserted in TABLE_1. No. of rows inserted = %s. %n", col1, col2, numRowsInserted);
        }
    }

    private void deleteRecordFromTable1(String col1, int col2) throws SQLException {
        try (PreparedStatement stmt = CON.prepareStatement("DELETE FROM TABLE_1 WHERE COL_1 = ? AND COL_2 = ?")) {
            stmt.setString(1, col1);
            stmt.setInt(2, col2);
            int numRowsDeleted = stmt.executeUpdate();
            System.out.printf("Record (%s, %s) deleted from TABLE_1. No. of rows deleted = %s. %n", col1, col2, numRowsDeleted);
        }
    }

    @Builder
    private record Table1Row(String col1, int col2) {}
}
