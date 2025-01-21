package org.sql4j.sql.query.integ;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;
import org.sql4j.sql.query.SqlQuery;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.sql4j.sql.query.Column.ALL;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SelectQueryExecuteTest extends SqlQueryExecuteTestBase {

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_singleColumn_singleTable(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

        List<String> results =
                SqlQuery.select(COL_1)
                        .from(TABLE_1)
                        .execute(connection, rs -> rs.getString(COL_1.getName()));
        assertEquals(RECORDS.size(), results.size());
        RECORDS.forEach(record -> assertTrue(results.contains(record.col1())));

        deleteRecordsFromTable1(RECORDS, connection);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_multipleColumns_singleTable(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

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
                        .col1(record.col1())
                        .col2(record.col2())
                        .build())
                .forEach(record -> assertTrue(results.contains(record)));

        deleteRecordsFromTable1(RECORDS, connection);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_multipleColumns_singleTable_singleCondition(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

        List<Table1Row> results =
                SqlQuery.select(COL_1, COL_2)
                        .from(TABLE_1)
                        .where(COL_1.equalTo("test1"))
                        .execute(connection, rs -> Table1Row.builder()
                                .col1(rs.getString(COL_1.getName()))
                                .col2(rs.getInt(COL_2.getName()))
                                .build());
        assertEquals(RECORDS.stream().filter(record -> record.col1().equals("test1")).count(), results.size());
        RECORDS.stream()
                .filter(record -> record.col1().equals("test1"))
                .map(record -> Table1Row.builder()
                        .col1(record.col1())
                        .col2(record.col2())
                        .build())
                .forEach(record -> assertTrue(results.contains(record)));

        deleteRecordsFromTable1(RECORDS, connection);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_multipleColumns_singleTable_singleCondition2(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

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
        assertEquals(RECORDS.stream().filter(record -> record.col2() == 10).count(), results.size());
        RECORDS.stream()
                .filter(record -> record.col2() == 10)
                .forEach(record -> assertTrue(results.contains(record)));

        deleteRecordsFromTable1(RECORDS, connection);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_multipleColumns_singleTable_singleCondition3(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

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
        assertEquals(RECORDS.stream().filter(record -> Objects.equals(record.col3(), 1.3)).count(), results.size());
        RECORDS.stream()
                .filter(record -> Objects.equals(record.col3(), 1.3))
                .forEach(record -> assertTrue(results.contains(record)));

        deleteRecordsFromTable1(RECORDS, connection);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_multipleColumns_singleTable_singleCondition4(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

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
        assertEquals(RECORDS.stream().filter(record -> Objects.equals(record.col4(), Date.valueOf("2025-01-04"))).count(), results.size());
        RECORDS.stream()
                .filter(record -> Objects.equals(record.col4(), Date.valueOf("2025-01-04")))
                .forEach(record -> assertTrue(results.contains(record)));

        deleteRecordsFromTable1(RECORDS, connection);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_multipleColumns_singleTable_singleCondition5(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

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
        assertEquals(RECORDS.stream().filter(record -> Objects.equals(record.col5(), Time.valueOf("01:01:05"))).count(), results.size());
        RECORDS.stream()
                .filter(record -> Objects.equals(record.col5(), Time.valueOf("01:01:05")))
                .forEach(record -> assertTrue(results.contains(record)));

        deleteRecordsFromTable1(RECORDS, connection);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_multipleColumns_singleTable_singleCondition6(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

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
        assertEquals(RECORDS.stream().filter(record -> Objects.equals(record.col6(), Timestamp.valueOf("2025-01-06 01:01:06"))).count(), results.size());
        RECORDS.stream()
                .filter(record -> Objects.equals(record.col6(), Timestamp.valueOf("2025-01-06 01:01:06")))
                .forEach(record -> assertTrue(results.contains(record)));

        deleteRecordsFromTable1(RECORDS, connection);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_multipleColumns_singleTable_singleCondition7(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

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
        assertEquals(RECORDS.stream().filter(record -> record.col4() != null && record.col4().before(Date.valueOf("2025-01-07"))).count(), results.size());
        RECORDS.stream()
                .filter(record -> record.col4() != null && record.col4().before(Date.valueOf("2025-01-07")))
                .forEach(record -> assertTrue(results.contains(record)));

        deleteRecordsFromTable1(RECORDS, connection);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_multipleColumns_singleTable_singleCondition8(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

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
        assertEquals(RECORDS.stream().filter(record -> record.col5() != null && record.col5().before(Time.valueOf("01:01:07"))).count(), results.size());
        RECORDS.stream()
                .filter(record -> record.col5() != null && record.col5().before(Time.valueOf("01:01:07")))
                .forEach(record -> assertTrue(results.contains(record)));

        deleteRecordsFromTable1(RECORDS, connection);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_multipleColumns_singleTable_singleCondition9(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

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
        assertEquals(RECORDS.stream().filter(record -> record.col6() != null && record.col6().before(Timestamp.valueOf("2025-01-06 01:01:07"))).count(), results.size());
        RECORDS.stream()
                .filter(record -> record.col6() != null && record.col6().before(Timestamp.valueOf("2025-01-06 01:01:07")))
                .forEach(record -> assertTrue(results.contains(record)));

        deleteRecordsFromTable1(RECORDS, connection);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_multipleColumns_singleTable_multipleConditions(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

        List<Table1Row> results =
                SqlQuery.select(COL_1, COL_2)
                        .from(TABLE_1)
                        .where(COL_1.equalTo("test1")
                                .and(COL_2.equalTo(10)))
                        .execute(connection, rs -> Table1Row.builder()
                                .col1(rs.getString(COL_1.getName()))
                                .col2(rs.getInt(COL_2.getName()))
                                .build());
        assertEquals(RECORDS.stream().filter(record -> record.col1().equals("test1") && record.col2() == 10).count(), results.size());
        RECORDS.stream()
                .filter(record -> record.col1().equals("test1") && record.col2() == 10)
                .map(record -> Table1Row.builder()
                        .col1(record.col1())
                        .col2(record.col2())
                        .build())
                .forEach(record -> assertTrue(results.contains(record)));

        deleteRecordsFromTable1(RECORDS, connection);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_multipleColumns_singleTable_multipleConditions2(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

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
        assertEquals(RECORDS.stream().filter(record -> record.col1().equals("test1") && record.col2() == 10).count(), results.size());
        RECORDS.stream()
                .filter(record -> record.col1().equals("test1") && record.col2() == 10)
                .forEach(record -> assertTrue(results.contains(record)));

        deleteRecordsFromTable1(RECORDS, connection);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_multipleColumns_singleTable_multipleConditions3(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

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
        assertEquals(RECORDS.stream().filter(record -> record.col1().equals("test1") && record.col2() == 10 && record.col6() != null && record.col6().before(Timestamp.valueOf("2025-01-06 01:01:07"))).count(), results.size());
        RECORDS.stream()
                .filter(record -> record.col1().equals("test1") && record.col2() == 10 && record.col6() != null && record.col6().before(Timestamp.valueOf("2025-01-06 01:01:07")))
                .forEach(record -> assertTrue(results.contains(record)));

        deleteRecordsFromTable1(RECORDS, connection);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_allColumns_singleTable(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

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

        deleteRecordsFromTable1(RECORDS, connection);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_allColumns_singleTable_count(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

        List<Integer> results =
                SqlQuery.select(ALL.count())
                        .from(TABLE_1)
                        .execute(connection, rs -> rs.getInt(ALL.count().getName()));
        assertEquals(1, results.size());
        assertEquals(RECORDS.size(), results.getFirst());

        deleteRecordsFromTable1(RECORDS, connection);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_allColumns_singleTable_count_condition_isNull(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

        List<Integer> results =
                SqlQuery.select(ALL.count())
                        .from(TABLE_1)
                        .where(COL_3.isNull())
                        .execute(connection, rs -> rs.getInt(ALL.count().getName()));
        assertEquals(1, results.size());
        assertEquals(RECORDS.stream().filter(record -> record.col3() == null).count(), results.getFirst().intValue());

        deleteRecordsFromTable1(RECORDS, connection);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_allColumns_singleTable_count_condition_isNotNull(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

        List<Integer> results =
                SqlQuery.select(ALL.count())
                        .from(TABLE_1)
                        .where(COL_3.isNotNull())
                        .execute(connection, rs -> rs.getInt(ALL.count().getName()));
        assertEquals(1, results.size());
        assertEquals(RECORDS.stream().filter(record -> record.col3() != null).count(), results.getFirst().intValue());

        deleteRecordsFromTable1(RECORDS, connection);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_allColumns_singleTable_count_condition_in(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

        List<Integer> results =
                SqlQuery.select(ALL.count())
                        .from(TABLE_1)
                        .where(COL_3.in(1.4, 1.5, 1.6))
                        .execute(connection, rs -> rs.getInt(ALL.count().getName()));
        assertEquals(1, results.size());
        assertEquals(RECORDS.stream().filter(record -> Arrays.asList(1.4, 1.5, 1.6).contains(record.col3())).count(), results.getFirst().intValue());

        deleteRecordsFromTable1(RECORDS, connection);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_allColumns_singleTable_count_condition_notIn(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

        List<Integer> results =
                SqlQuery.select(ALL.count())
                        .from(TABLE_1)
                        .where(COL_3.isNull()
                                .or(COL_3.notIn(1.4, 1.5, 1.6)))
                        .execute(connection, rs -> rs.getInt(ALL.count().getName()));
        assertEquals(1, results.size());
        assertEquals(RECORDS.stream().filter(record -> !Arrays.asList(1.4, 1.5, 1.6).contains(record.col3())).count(), results.getFirst().intValue());

        deleteRecordsFromTable1(RECORDS, connection);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_singleColumn_singleTable_count(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

        List<Integer> results =
                SqlQuery.select(COL_1.count())
                        .from(TABLE_1)
                        .execute(connection, rs -> rs.getInt(COL_1.count().getName()));
        assertEquals(1, results.size());
        assertEquals(RECORDS.size(), results.getFirst());

        deleteRecordsFromTable1(RECORDS, connection);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_singleColumn_singleTable_distinct(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

        List<String> results =
                SqlQuery.select(COL_1.distinct())
                        .from(TABLE_1)
                        .execute(connection, rs -> rs.getString(COL_1.getName()));
        assertEquals(RECORDS.size(), results.size());
        RECORDS.forEach(record -> assertTrue(results.contains(record.col1())));

        deleteRecordsFromTable1(RECORDS, connection);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_singleColumn_singleTable_distinct2(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

        List<Integer> results =
                SqlQuery.select(COL_2.distinct())
                        .from(TABLE_1)
                        .execute(connection, rs -> rs.getInt(COL_2.getName()));
        assertEquals(RECORDS.stream().map(Table1Row::col2).distinct().count(), results.size());
        RECORDS.stream()
                .map(Table1Row::col2)
                .distinct()
                .forEach(col2 -> assertTrue(results.contains(col2)));

        deleteRecordsFromTable1(RECORDS, connection);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testSelectQuery_singleColumn_singleTable_countDistinct(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

        List<Integer> results =
                SqlQuery.select(COL_2.countDistinct())
                        .from(TABLE_1)
                        .execute(connection, rs -> rs.getInt(COL_2.countDistinct().getName()));
        assertEquals(1, results.size());
        assertEquals(RECORDS.stream().map(Table1Row::col2).distinct().count(), results.getFirst().intValue());

        deleteRecordsFromTable1(RECORDS, connection);
    }
}
