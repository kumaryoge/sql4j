package org.sql4j.sql.query;

import lombok.Builder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.sql4j.sql.query.Column.ALL;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SelectQueryExecuteTest {
    private static final Table TABLE_1 = Table.forName("TABLE_1");
    private static final Column<String> COL_1 = Column.forName("COL_1");
    private static final Column<Integer> COL_2 = Column.forName("COL_2");
    private static final Column<Double> COL_3 = Column.forName("COL_3");
    private static final Column<Date> COL_4 = Column.forName("COL_4");
    private static final Column<Time> COL_5 = Column.forName("COL_5");
    private static final Column<Timestamp> COL_6 = Column.forName("COL_6");

    private static final List<Table1Row> RECORDS = List.of(
            Table1Row.builder().col1("test1").col2(10).build(),
            Table1Row.builder().col1("test2").col2(10).col3(1.3).build(),
            Table1Row.builder().col1("test3").col2(30).col3(1.4).col4(Date.valueOf("2025-01-04")).build(),
            Table1Row.builder().col1("test4").col2(40).col3(1.5).col4(Date.valueOf("2025-01-05")).col5(Time.valueOf("01:01:05")).build(),
            Table1Row.builder().col1("test5").col2(50).col3(1.6).col4(Date.valueOf("2025-01-06")).col5(Time.valueOf("01:01:06")).col6(Timestamp.valueOf("2025-01-06 01:01:06")).build()
    );

    private static final List<Connection> CONNECTIONS = new ArrayList<>();

    @BeforeAll
    public static void setUp() throws ClassNotFoundException, SQLException {
        connectToH2Database();
        connectToMySqlDatabase();

        for (Connection connection : CONNECTIONS) {
            System.out.println("\nSetUp using connection: " + connection);

            try (Statement stmt = connection.createStatement()) {
                stmt.execute("""
                    CREATE TABLE IF NOT EXISTS TABLE_1 (
                        COL_1 VARCHAR(255) PRIMARY KEY,
                        COL_2 INT NOT NULL,
                        COL_3 DOUBLE,
                        COL_4 DATE,
                        COL_5 TIME,
                        COL_6 TIMESTAMP
                    )""");
                System.out.println("TABLE_1 created.");
            }

            for (Table1Row record : RECORDS) {
                insertRecordInTable1(record, connection);
            }
        }
    }

    @AfterAll
    public static void tearDown() throws SQLException {
        for (Connection connection : CONNECTIONS) {
            System.out.println("\nTearDown using connection: " + connection);

            for (Table1Row record : RECORDS) {
                deleteRecordFromTable1(record, connection);
            }

            try (Statement stmt = connection.createStatement()) {
                stmt.executeUpdate("DROP TABLE IF EXISTS TABLE_1");
                System.out.println("TABLE_1 dropped.");
            }
            // Close the connection
            connection.close();
        }
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_singleColumn_singleTable(Connection connection) throws SQLException {
        List<String> results =
                SqlQuery.select(COL_1)
                        .from(TABLE_1)
                        .execute(connection, rs -> rs.getString(COL_1.getName()));
        assertEquals(RECORDS.size(), results.size());
        RECORDS.forEach(record -> assertTrue(results.contains(record.col1)));
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_multipleColumns_singleTable(Connection connection) throws SQLException {
        List<Table1Row> results =
                SqlQuery.select(COL_1, COL_2)
                        .from(TABLE_1)
                        .execute(connection, rs -> Table1Row.builder()
                                .col1(rs.getString(COL_1.getName()))
                                .col2(rs.getInt(COL_2.getName()))
                                .build());
        assertEquals(RECORDS.size(), results.size());
        RECORDS.stream()
                .map(record -> Table1Row.builder()
                        .col1(record.col1)
                        .col2(record.col2)
                        .build())
                .forEach(record -> assertTrue(results.contains(record)));
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_multipleColumns_singleTable_singleCondition(Connection connection) throws SQLException {
        List<Table1Row> results =
                SqlQuery.select(COL_1, COL_2)
                        .from(TABLE_1)
                        .where(COL_1.equalTo("test1"))
                        .execute(connection, rs -> Table1Row.builder()
                                .col1(rs.getString(COL_1.getName()))
                                .col2(rs.getInt(COL_2.getName()))
                                .build());
        assertEquals(RECORDS.stream().filter(record -> record.col1.equals("test1")).count(), results.size());
        RECORDS.stream()
                .filter(record -> record.col1.equals("test1"))
                .map(record -> Table1Row.builder()
                        .col1(record.col1)
                        .col2(record.col2)
                        .build())
                .forEach(record -> assertTrue(results.contains(record)));
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_multipleColumns_singleTable_singleCondition2(Connection connection) throws SQLException {
        List<Table1Row> results =
                SqlQuery.select(ALL)
                        .from(TABLE_1)
                        .where(COL_2.equalTo(10))
                        .execute(connection, rs -> Table1Row.builder()
                                .col1(rs.getString(COL_1.getName()))
                                .col2(rs.getInt(COL_2.getName()))
                                .col3(rs.getObject(COL_3.getName(), Double.class))
                                .col4(rs.getDate(COL_4.getName()))
                                .col5(rs.getTime(COL_5.getName()))
                                .col6(rs.getTimestamp(COL_6.getName()))
                                .build());
        assertEquals(RECORDS.stream().filter(record -> record.col2 == 10).count(), results.size());
        RECORDS.stream()
                .filter(record -> record.col2 == 10)
                .forEach(record -> assertTrue(results.contains(record)));
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_multipleColumns_singleTable_singleCondition3(Connection connection) throws SQLException {
        List<Table1Row> results =
                SqlQuery.select(ALL)
                        .from(TABLE_1)
                        .where(COL_3.equalTo(1.3))
                        .execute(connection, rs -> Table1Row.builder()
                                .col1(rs.getString(COL_1.getName()))
                                .col2(rs.getInt(COL_2.getName()))
                                .col3(rs.getObject(COL_3.getName(), Double.class))
                                .col4(rs.getDate(COL_4.getName()))
                                .col5(rs.getTime(COL_5.getName()))
                                .col6(rs.getTimestamp(COL_6.getName()))
                                .build());
        assertEquals(RECORDS.stream().filter(record -> Objects.equals(record.col3, 1.3)).count(), results.size());
        RECORDS.stream()
                .filter(record -> Objects.equals(record.col3, 1.3))
                .forEach(record -> assertTrue(results.contains(record)));
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_multipleColumns_singleTable_singleCondition4(Connection connection) throws SQLException {
        List<Table1Row> results =
                SqlQuery.select(ALL)
                        .from(TABLE_1)
                        .where(COL_4.equalTo(Date.valueOf("2025-01-04")))
                        .execute(connection, rs -> Table1Row.builder()
                                .col1(rs.getString(COL_1.getName()))
                                .col2(rs.getInt(COL_2.getName()))
                                .col3(rs.getObject(COL_3.getName(), Double.class))
                                .col4(rs.getDate(COL_4.getName()))
                                .col5(rs.getTime(COL_5.getName()))
                                .col6(rs.getTimestamp(COL_6.getName()))
                                .build());
        assertEquals(RECORDS.stream().filter(record -> Objects.equals(record.col4, Date.valueOf("2025-01-04"))).count(), results.size());
        RECORDS.stream()
                .filter(record -> Objects.equals(record.col4, Date.valueOf("2025-01-04")))
                .forEach(record -> assertTrue(results.contains(record)));
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_multipleColumns_singleTable_singleCondition5(Connection connection) throws SQLException {
        List<Table1Row> results =
                SqlQuery.select(ALL)
                        .from(TABLE_1)
                        .where(COL_5.equalTo(Time.valueOf("01:01:05")))
                        .execute(connection, rs -> Table1Row.builder()
                                .col1(rs.getString(COL_1.getName()))
                                .col2(rs.getInt(COL_2.getName()))
                                .col3(rs.getObject(COL_3.getName(), Double.class))
                                .col4(rs.getDate(COL_4.getName()))
                                .col5(rs.getTime(COL_5.getName()))
                                .col6(rs.getTimestamp(COL_6.getName()))
                                .build());
        assertEquals(RECORDS.stream().filter(record -> Objects.equals(record.col5, Time.valueOf("01:01:05"))).count(), results.size());
        RECORDS.stream()
                .filter(record -> Objects.equals(record.col5, Time.valueOf("01:01:05")))
                .forEach(record -> assertTrue(results.contains(record)));
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_multipleColumns_singleTable_singleCondition6(Connection connection) throws SQLException {
        List<Table1Row> results =
                SqlQuery.select(ALL)
                        .from(TABLE_1)
                        .where(COL_6.equalTo(Timestamp.valueOf("2025-01-06 01:01:06")))
                        .execute(connection, rs -> Table1Row.builder()
                                .col1(rs.getString(COL_1.getName()))
                                .col2(rs.getInt(COL_2.getName()))
                                .col3(rs.getObject(COL_3.getName(), Double.class))
                                .col4(rs.getDate(COL_4.getName()))
                                .col5(rs.getTime(COL_5.getName()))
                                .col6(rs.getTimestamp(COL_6.getName()))
                                .build());
        assertEquals(RECORDS.stream().filter(record -> Objects.equals(record.col6, Timestamp.valueOf("2025-01-06 01:01:06"))).count(), results.size());
        RECORDS.stream()
                .filter(record -> Objects.equals(record.col6, Timestamp.valueOf("2025-01-06 01:01:06")))
                .forEach(record -> assertTrue(results.contains(record)));
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_multipleColumns_singleTable_singleCondition7(Connection connection) throws SQLException {
        List<Table1Row> results =
                SqlQuery.select(ALL)
                        .from(TABLE_1)
                        .where(COL_4.lessThan(Date.valueOf("2025-01-07")))
                        .execute(connection, rs -> Table1Row.builder()
                                .col1(rs.getString(COL_1.getName()))
                                .col2(rs.getInt(COL_2.getName()))
                                .col3(rs.getObject(COL_3.getName(), Double.class))
                                .col4(rs.getDate(COL_4.getName()))
                                .col5(rs.getTime(COL_5.getName()))
                                .col6(rs.getTimestamp(COL_6.getName()))
                                .build());
        assertEquals(RECORDS.stream().filter(record -> record.col4 != null && record.col4.before(Date.valueOf("2025-01-07"))).count(), results.size());
        RECORDS.stream()
                .filter(record -> record.col4 != null && record.col4.before(Date.valueOf("2025-01-07")))
                .forEach(record -> assertTrue(results.contains(record)));
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_multipleColumns_singleTable_singleCondition8(Connection connection) throws SQLException {
        List<Table1Row> results =
                SqlQuery.select(ALL)
                        .from(TABLE_1)
                        .where(COL_5.lessThan(Time.valueOf("01:01:07")))
                        .execute(connection, rs -> Table1Row.builder()
                                .col1(rs.getString(COL_1.getName()))
                                .col2(rs.getInt(COL_2.getName()))
                                .col3(rs.getObject(COL_3.getName(), Double.class))
                                .col4(rs.getDate(COL_4.getName()))
                                .col5(rs.getTime(COL_5.getName()))
                                .col6(rs.getTimestamp(COL_6.getName()))
                                .build());
        assertEquals(RECORDS.stream().filter(record -> record.col5 != null && record.col5.before(Time.valueOf("01:01:07"))).count(), results.size());
        RECORDS.stream()
                .filter(record -> record.col5 != null && record.col5.before(Time.valueOf("01:01:07")))
                .forEach(record -> assertTrue(results.contains(record)));
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_multipleColumns_singleTable_singleCondition9(Connection connection) throws SQLException {
        List<Table1Row> results =
                SqlQuery.select(ALL)
                        .from(TABLE_1)
                        .where(COL_6.lessThan(Timestamp.valueOf("2025-01-06 01:01:07")))
                        .execute(connection, rs -> Table1Row.builder()
                                .col1(rs.getString(COL_1.getName()))
                                .col2(rs.getInt(COL_2.getName()))
                                .col3(rs.getObject(COL_3.getName(), Double.class))
                                .col4(rs.getDate(COL_4.getName()))
                                .col5(rs.getTime(COL_5.getName()))
                                .col6(rs.getTimestamp(COL_6.getName()))
                                .build());
        assertEquals(RECORDS.stream().filter(record -> record.col6 != null && record.col6.before(Timestamp.valueOf("2025-01-06 01:01:07"))).count(), results.size());
        RECORDS.stream()
                .filter(record -> record.col6 != null && record.col6.before(Timestamp.valueOf("2025-01-06 01:01:07")))
                .forEach(record -> assertTrue(results.contains(record)));
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_multipleColumns_singleTable_multipleConditions(Connection connection) throws SQLException {
        List<Table1Row> results =
                SqlQuery.select(COL_1, COL_2)
                        .from(TABLE_1)
                        .where(COL_1.equalTo("test1")
                                .and(COL_2.equalTo(10)))
                        .execute(connection, rs -> Table1Row.builder()
                                .col1(rs.getString(COL_1.getName()))
                                .col2(rs.getInt(COL_2.getName()))
                                .build());
        assertEquals(RECORDS.stream().filter(record -> record.col1.equals("test1") && record.col2 == 10).count(), results.size());
        RECORDS.stream()
                .filter(record -> record.col1.equals("test1") && record.col2 == 10)
                .map(record -> Table1Row.builder()
                        .col1(record.col1)
                        .col2(record.col2)
                        .build())
                .forEach(record -> assertTrue(results.contains(record)));
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_multipleColumns_singleTable_multipleConditions2(Connection connection) throws SQLException {
        List<Table1Row> results =
                SqlQuery.select(ALL)
                        .from(TABLE_1)
                        .where(COL_1.equalTo("test1")
                                .and(COL_2.equalTo(10)))
                        .execute(connection, rs -> Table1Row.builder()
                                .col1(rs.getString(COL_1.getName()))
                                .col2(rs.getInt(COL_2.getName()))
                                .col3(rs.getObject(COL_3.getName(), Double.class))
                                .col4(rs.getDate(COL_4.getName()))
                                .col5(rs.getTime(COL_5.getName()))
                                .col6(rs.getTimestamp(COL_6.getName()))
                                .build());
        assertEquals(RECORDS.stream().filter(record -> record.col1.equals("test1") && record.col2 == 10).count(), results.size());
        RECORDS.stream()
                .filter(record -> record.col1.equals("test1") && record.col2 == 10)
                .forEach(record -> assertTrue(results.contains(record)));
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_multipleColumns_singleTable_multipleConditions3(Connection connection) throws SQLException {
        List<Table1Row> results =
                SqlQuery.select(ALL)
                        .from(TABLE_1)
                        .where(COL_1.equalTo("test1")
                                .and(COL_2.equalTo(10))
                                .and(COL_6.lessThan(Timestamp.valueOf("2025-01-06 01:01:07"))))
                        .execute(connection, rs -> Table1Row.builder()
                                .col1(rs.getString(COL_1.getName()))
                                .col2(rs.getInt(COL_2.getName()))
                                .col3(rs.getObject(COL_3.getName(), Double.class))
                                .col4(rs.getDate(COL_4.getName()))
                                .col5(rs.getTime(COL_5.getName()))
                                .col6(rs.getTimestamp(COL_6.getName()))
                                .build());
        assertEquals(RECORDS.stream().filter(record -> record.col1.equals("test1") && record.col2 == 10 && record.col6 != null && record.col6.before(Timestamp.valueOf("2025-01-06 01:01:07"))).count(), results.size());
        RECORDS.stream()
                .filter(record -> record.col1.equals("test1") && record.col2 == 10 && record.col6 != null && record.col6.before(Timestamp.valueOf("2025-01-06 01:01:07")))
                .forEach(record -> assertTrue(results.contains(record)));
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_allColumns_singleTable(Connection connection) throws SQLException {
        List<Table1Row> results =
                SqlQuery.select(ALL)
                        .from(TABLE_1)
                        .execute(connection, rs -> Table1Row.builder()
                                .col1(rs.getString(COL_1.getName()))
                                .col2(rs.getInt(COL_2.getName()))
                                .col3(rs.getObject(COL_3.getName(), Double.class))
                                .col4(rs.getDate(COL_4.getName()))
                                .col5(rs.getTime(COL_5.getName()))
                                .col6(rs.getTimestamp(COL_6.getName()))
                                .build());
        assertEquals(RECORDS.size(), results.size());
        RECORDS.forEach(record -> assertTrue(results.contains(record)));
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_allColumns_singleTable_count(Connection connection) throws SQLException {
        List<Integer> results =
                SqlQuery.select(ALL.count())
                        .from(TABLE_1)
                        .execute(connection, rs -> rs.getInt(ALL.count().getName()));
        assertEquals(1, results.size());
        assertEquals(RECORDS.size(), results.getFirst());
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_allColumns_singleTable_count_condition_isNull(Connection connection) throws SQLException {
        List<Integer> results =
                SqlQuery.select(ALL.count())
                        .from(TABLE_1)
                        .where(COL_3.isNull())
                        .execute(connection, rs -> rs.getInt(ALL.count().getName()));
        assertEquals(1, results.size());
        assertEquals(RECORDS.stream().filter(record -> record.col3 == null).count(), results.getFirst().intValue());
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_allColumns_singleTable_count_condition_isNotNull(Connection connection) throws SQLException {
        List<Integer> results =
                SqlQuery.select(ALL.count())
                        .from(TABLE_1)
                        .where(COL_3.isNotNull())
                        .execute(connection, rs -> rs.getInt(ALL.count().getName()));
        assertEquals(1, results.size());
        assertEquals(RECORDS.stream().filter(record -> record.col3 != null).count(), results.getFirst().intValue());
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_allColumns_singleTable_count_condition_in(Connection connection) throws SQLException {
        List<Integer> results =
                SqlQuery.select(ALL.count())
                        .from(TABLE_1)
                        .where(COL_3.in(1.4, 1.5, 1.6))
                        .execute(connection, rs -> rs.getInt(ALL.count().getName()));
        assertEquals(1, results.size());
        assertEquals(RECORDS.stream().filter(record -> Arrays.asList(1.4, 1.5, 1.6).contains(record.col3)).count(), results.getFirst().intValue());
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_allColumns_singleTable_count_condition_notIn(Connection connection) throws SQLException {
        List<Integer> results =
                SqlQuery.select(ALL.count())
                        .from(TABLE_1)
                        .where(COL_3.isNull()
                                .or(COL_3.notIn(1.4, 1.5, 1.6)))
                        .execute(connection, rs -> rs.getInt(ALL.count().getName()));
        assertEquals(1, results.size());
        assertEquals(RECORDS.stream().filter(record -> !Arrays.asList(1.4, 1.5, 1.6).contains(record.col3)).count(), results.getFirst().intValue());
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_singleColumn_singleTable_count(Connection connection) throws SQLException {
        List<Integer> results =
                SqlQuery.select(COL_1.count())
                        .from(TABLE_1)
                        .execute(connection, rs -> rs.getInt(COL_1.count().getName()));
        assertEquals(1, results.size());
        assertEquals(RECORDS.size(), results.getFirst());
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_singleColumn_singleTable_distinct(Connection connection) throws SQLException {
        List<String> results =
                SqlQuery.select(COL_1.distinct())
                        .from(TABLE_1)
                        .execute(connection, rs -> rs.getString(COL_1.getName()));
        assertEquals(RECORDS.size(), results.size());
        RECORDS.forEach(record -> assertTrue(results.contains(record.col1)));
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_singleColumn_singleTable_distinct2(Connection connection) throws SQLException {
        List<Integer> results =
                SqlQuery.select(COL_2.distinct())
                        .from(TABLE_1)
                        .execute(connection, rs -> rs.getInt(COL_2.getName()));
        assertEquals(RECORDS.stream().map(record -> record.col2).distinct().count(), results.size());
        RECORDS.stream()
                .map(record -> record.col2)
                .distinct()
                .forEach(col2 -> assertTrue(results.contains(col2)));
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_singleColumn_singleTable_countDistinct(Connection connection) throws SQLException {
        List<Integer> results =
                SqlQuery.select(COL_2.countDistinct())
                        .from(TABLE_1)
                        .execute(connection, rs -> rs.getInt(COL_2.countDistinct().getName()));
        assertEquals(1, results.size());
        assertEquals(RECORDS.stream().map(record -> record.col2).distinct().count(), results.getFirst().intValue());
    }

    private static void connectToH2Database() throws ClassNotFoundException, SQLException {
        // Load the H2 driver
        Class.forName("org.h2.Driver");

        // Connect to the in-memory database
        String url = "jdbc:h2:mem:testdb"; // testdb is the database name
        String user = "sa";
        String password = "";
        CONNECTIONS.add(DriverManager.getConnection(url, user, password));
        System.out.println("Connected to H2 database!");
    }

    private static void connectToMySqlDatabase() throws ClassNotFoundException, SQLException {
        // Load the mysql driver
        Class.forName("com.mysql.cj.jdbc.Driver");

        // Connect to the mysql database
        String url = "jdbc:mysql://localhost:3306/testdb"; // testdb is the database name
        String user = System.getenv("DB_USER");
        String password = System.getenv("DB_PASS");
        CONNECTIONS.add(DriverManager.getConnection(url, user, password));
        System.out.println("Connected to MySql database!");
    }

    private static void insertRecordInTable1(Table1Row record, Connection connection) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("INSERT INTO TABLE_1 (COL_1, COL_2, COL_3, COL_4, COL_5, COL_6) VALUES (?, ?, ?, ?, ?, ?)")) {
            stmt.setString(1, record.col1);
            stmt.setInt(2, record.col2);
            stmt.setObject(3, record.col3, Types.DOUBLE);
            stmt.setDate(4, record.col4);
            stmt.setTime(5, record.col5);
            stmt.setTimestamp(6, record.col6);
            stmt.executeUpdate();
            System.out.printf("Inserted record (%s, %s, %s, %s, %s, %s) in TABLE_1.%n",
                    record.col1, record.col2, record.col3, record.col4, record.col5, record.col6);
        }
    }

    private static void deleteRecordFromTable1(Table1Row record, Connection connection) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("DELETE FROM TABLE_1 WHERE COL_1 = ? AND COL_2 = ? AND COL_3 = ? AND COL_4 = ? AND COL_5 = ? AND COL_6 = ?")) {
            stmt.setString(1, record.col1);
            stmt.setInt(2, record.col2);
            stmt.setObject(3, record.col3, Types.DOUBLE);
            stmt.setDate(4, record.col4);
            stmt.setTime(5, record.col5);
            stmt.setTimestamp(6, record.col6);
            stmt.executeUpdate();
            System.out.printf("Deleted record (%s, %s, %s, %s, %s, %s) from TABLE_1.%n",
                    record.col1, record.col2, record.col3, record.col4, record.col5, record.col6);
        }
    }

    @Builder
    private record Table1Row(String col1, int col2, Double col3, Date col4, Time col5, Timestamp col6) {}
}
