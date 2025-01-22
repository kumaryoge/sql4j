package org.sql4j.sql.query.integ;

import lombok.Builder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.sql4j.sql.query.Column;
import org.sql4j.sql.query.Table;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public abstract class SqlQueryExecuteTestBase {
    protected static final Table TABLE_1 = Table.forName("TABLE_1");
    protected static final Column<String> COL_1 = Column.forName("COL_1");
    protected static final Column<Integer> COL_2 = Column.forName("COL_2");
    protected static final Column<Double> COL_3 = Column.forName("COL_3");
    protected static final Column<Date> COL_4 = Column.forName("COL_4");
    protected static final Column<Time> COL_5 = Column.forName("COL_5");
    protected static final Column<Timestamp> COL_6 = Column.forName("COL_6");

    protected static final List<Table1Row> RECORDS = List.of(
            Table1Row.builder().col1("test1").col2(10).build(),
            Table1Row.builder().col1("test2").col2(10).col3(1.3).build(),
            Table1Row.builder().col1("test3").col2(30).col3(1.4).col4(Date.valueOf("2025-01-04")).build(),
            Table1Row.builder().col1("test4").col2(40).col3(1.5).col4(Date.valueOf("2025-01-05")).col5(Time.valueOf("01:01:05")).build(),
            Table1Row.builder().col1("test5").col2(50).col3(1.6).col4(Date.valueOf("2025-01-06")).col5(Time.valueOf("01:01:06")).col6(Timestamp.valueOf("2025-01-06 01:01:06")).build()
    );

    protected static final List<Connection> CONNECTIONS = new ArrayList<>();

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
        }
    }

    @AfterAll
    public static void tearDown() throws SQLException {
        for (Connection connection : CONNECTIONS) {
            System.out.println("\nTearDown using connection: " + connection);

            try (Statement stmt = connection.createStatement()) {
                stmt.executeUpdate("DROP TABLE IF EXISTS TABLE_1");
                System.out.println("TABLE_1 dropped.");
            }
            // Close the connection
            connection.close();
        }
        CONNECTIONS.clear();
    }

    private static void connectToH2Database() throws ClassNotFoundException {
        // Load the H2 driver
        Class.forName("org.h2.Driver");

        // Connect to the in-memory database
        String url = "jdbc:h2:mem:testdb"; // testdb is the database name
        String user = "sa";
        String password = "";
        try {
            CONNECTIONS.add(DriverManager.getConnection(url, user, password));
            System.out.println("Connected to H2 database!");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void connectToMySqlDatabase() throws ClassNotFoundException {
        // Load the mysql driver
        Class.forName("com.mysql.cj.jdbc.Driver");

        // Connect to the mysql database
        String url = "jdbc:mysql://localhost:3306/testdb"; // testdb is the database name
        String user = System.getenv("DB_USER");
        String password = System.getenv("DB_PASS");
        try {
            CONNECTIONS.add(DriverManager.getConnection(url, user, password));
            System.out.println("Connected to MySql database!");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    protected void insertRecordsInTable1(List<Table1Row> records, Connection connection) throws SQLException {
        System.out.println("\nUsing connection: " + connection);

        for (Table1Row record : records) {
            insertRecordInTable1(record, connection);
        }
    }

    protected void deleteRecordsFromTable1(List<Table1Row> records, Connection connection) throws SQLException {
        System.out.println("\nUsing connection: " + connection);

        for (Table1Row record : records) {
            deleteRecordFromTable1(record, connection);
        }
    }

    private void insertRecordInTable1(Table1Row record, Connection connection) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("INSERT INTO TABLE_1 (COL_1, COL_2, COL_3, COL_4, COL_5, COL_6) VALUES (?, ?, ?, ?, ?, ?)")) {
            stmt.setString(1, record.col1);
            stmt.setInt(2, record.col2);
            stmt.setObject(3, record.col3, Types.DOUBLE);
            stmt.setDate(4, record.col4);
            stmt.setTime(5, record.col5);
            stmt.setTimestamp(6, record.col6);
            int rowCount = stmt.executeUpdate();
            System.out.printf("Inserted %s record (%s, %s, %s, %s, %s, %s) in TABLE_1.%n",
                    rowCount, record.col1, record.col2, record.col3, record.col4, record.col5, record.col6);
        }
    }

    private void deleteRecordFromTable1(Table1Row record, Connection connection) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("DELETE FROM TABLE_1 WHERE COL_1 = ?")) {
            stmt.setString(1, record.col1);
            int rowCount = stmt.executeUpdate();
            System.out.printf("Deleted %s record (%s, %s, %s, %s, %s, %s) from TABLE_1.%n",
                    rowCount, record.col1, record.col2, record.col3, record.col4, record.col5, record.col6);
        }
    }

    @Builder
    protected record Table1Row(String col1, int col2, Double col3, Date col4, Time col5, Timestamp col6) {}
}
