package org.sql4j.sql.query;

import org.junit.jupiter.api.Test;

import java.sql.Date;

import static org.sql4j.sql.query.Column.*;
import static org.sql4j.sql.query.Table.TABLE_1;
import static org.sql4j.sql.query.Table.TABLE_2;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SelectQueryBuildTest {
    private static final Table T_1 = TABLE_1.as("T_1");
    private static final Table T_2 = TABLE_2.as("T_2");
    private static final Column<String> C_1 = COL_1.of(T_1).as("C_1");

    @Test
    void testSelectQuery_singleColumn_singleTable() {
        String expectedSql = """
                SELECT
                    COL_1
                FROM
                    TABLE_1
                ;""";

        String actualSql =
                SqlQuery.select(COL_1)
                        .from(TABLE_1)
                        .build();

        assertEquals(expectedSql, actualSql);
    }

    @Test
    void testSelectQuery_multipleColumns_singleTable() {
        String expectedSql = """
                SELECT
                    COL_1,
                    COL_2
                FROM
                    TABLE_1
                ;""";

        String actualSql =
                SqlQuery.select(COL_1, COL_2)
                        .from(TABLE_1)
                        .build();

        assertEquals(expectedSql, actualSql);
    }

    @Test
    void testSelectQuery_multipleColumns_multipleTables() {
        String expectedSql = """
                SELECT
                    COL_1,
                    COL_2,
                    COL_3,
                    COL_4
                FROM
                    TABLE_1,
                    TABLE_2
                ;""";

        String actualSql =
                SqlQuery.select(COL_1, COL_2, COL_3, COL_4)
                        .from(TABLE_1, TABLE_2)
                        .build();

        assertEquals(expectedSql, actualSql);
    }

    @Test
    void testSelectQuery_allColumns_singleTable() {
        String expectedSql = """
                SELECT
                    *
                FROM
                    TABLE_1
                ;""";

        String actualSql =
                SqlQuery.select(ALL)
                        .from(TABLE_1)
                        .build();

        assertEquals(expectedSql, actualSql);
    }

    @Test
    void testSelectQuery_allColumns_singleTable_count() {
        String expectedSql = """
                SELECT
                    COUNT(*)
                FROM
                    TABLE_1
                ;""";

        String actualSql =
                SqlQuery.select(ALL.count())
                        .from(TABLE_1)
                        .build();

        assertEquals(expectedSql, actualSql);
    }

    @Test
    void testSelectQuery_singleColumn_singleTable_count() {
        String expectedSql = """
                SELECT
                    COUNT(COL_1)
                FROM
                    TABLE_1
                ;""";

        String actualSql =
                SqlQuery.select(COL_1.count())
                        .from(TABLE_1)
                        .build();

        assertEquals(expectedSql, actualSql);
    }

    @Test
    void testSelectQuery_singleColumn_singleTable_distinct() {
        String expectedSql = """
                SELECT
                    DISTINCT COL_1
                FROM
                    TABLE_1
                ;""";

        String actualSql =
                SqlQuery.select(COL_1.distinct())
                        .from(TABLE_1)
                        .build();

        assertEquals(expectedSql, actualSql);
    }

    @Test
    void testSelectQuery_singleColumn_singleTable_distinct2() {
        String expectedSql = """
                SELECT
                    DISTINCT COL_2
                FROM
                    TABLE_1
                ;""";

        String actualSql =
                SqlQuery.select(COL_2.distinct())
                        .from(TABLE_1)
                        .build();

        assertEquals(expectedSql, actualSql);
    }

    @Test
    void testSelectQuery_singleColumn_singleTable_countDistinct() {
        String expectedSql = """
                SELECT
                    COUNT(DISTINCT COL_2)
                FROM
                    TABLE_1
                ;""";

        String actualSql =
                SqlQuery.select(COL_2.countDistinct())
                        .from(TABLE_1)
                        .build();

        assertEquals(expectedSql, actualSql);
    }

    @Test
    void testSelectQuery_singleCondition() {
        String expectedSql = """
                SELECT
                    COL_1,
                    COL_2,
                    COL_3,
                    COL_4
                FROM
                    TABLE_1,
                    TABLE_2
                WHERE
                    COL_1 = 'test'
                ;""";

        String actualSql =
                SqlQuery.select(COL_1, COL_2, COL_3, COL_4)
                        .from(TABLE_1, TABLE_2)
                        .where(COL_1.equalTo("test"))
                        .build();

        assertEquals(expectedSql, actualSql);
    }

    @Test
    void testSelectQuery_multipleConditions() {
        Date currentDate = new Date(System.currentTimeMillis());

        String expectedSql = """
                SELECT
                    COL_1,
                    COL_2,
                    COL_3,
                    COL_4
                FROM
                    TABLE_1,
                    TABLE_2
                WHERE
                    COL_1 = 'test'
                    AND COL_2 = 1
                     OR COL_3 = 2.0
                     OR COL_4 = '%s'
                ;""".formatted(currentDate);

        String actualSql =
                SqlQuery.select(COL_1, COL_2, COL_3, COL_4)
                        .from(TABLE_1, TABLE_2)
                        .where(COL_1.equalTo("test")
                                .and(COL_2.equalTo(1))
                                .or(COL_3.equalTo(2.0))
                                .or(COL_4.equalTo(currentDate)))
                        .build();

        assertEquals(expectedSql, actualSql);
    }

    @Test
    void testSelectQuery_groupBySingleColumn() {
        Date currentDate = new Date(System.currentTimeMillis());

        String expectedSql = """
                SELECT
                    COL_1,
                    COL_2,
                    COL_3,
                    COL_4
                FROM
                    TABLE_1,
                    TABLE_2
                WHERE
                    COL_1 = 'test'
                    AND COL_2 = 1
                     OR COL_3 = 2.0
                     OR COL_4 = '%s'
                GROUP BY
                    COL_1
                ;""".formatted(currentDate);

        String actualSql =
                SqlQuery.select(COL_1, COL_2, COL_3, COL_4)
                        .from(TABLE_1, TABLE_2)
                        .where(COL_1.equalTo("test")
                                .and(COL_2.equalTo(1))
                                .or(COL_3.equalTo(2.0))
                                .or(COL_4.equalTo(currentDate)))
                        .groupBy(COL_1)
                        .build();

        assertEquals(expectedSql, actualSql);
    }

    @Test
    void testSelectQuery_groupByMultipleColumns() {
        Date currentDate = new Date(System.currentTimeMillis());

        String expectedSql = """
                SELECT
                    COL_1,
                    COL_2,
                    COL_3,
                    COL_4
                FROM
                    TABLE_1,
                    TABLE_2
                WHERE
                    COL_1 = 'test'
                    AND COL_2 = 1
                     OR COL_3 = 2.0
                     OR COL_4 = '%s'
                GROUP BY
                    COL_1,
                    COL_2
                ;""".formatted(currentDate);

        String actualSql =
                SqlQuery.select(COL_1, COL_2, COL_3, COL_4)
                        .from(TABLE_1, TABLE_2)
                        .where(COL_1.equalTo("test")
                                .and(COL_2.equalTo(1))
                                .or(COL_3.equalTo(2.0))
                                .or(COL_4.equalTo(currentDate)))
                        .groupBy(COL_1, COL_2)
                        .build();

        assertEquals(expectedSql, actualSql);
    }

    @Test
    void testSelectQuery_orderBySingleColumn() {
        Date currentDate = new Date(System.currentTimeMillis());

        String expectedSql = """
                SELECT
                    COL_1,
                    COL_2,
                    COL_3,
                    COL_4
                FROM
                    TABLE_1,
                    TABLE_2
                WHERE
                    COL_1 = 'test'
                    AND COL_2 = 1
                     OR COL_3 = 2.0
                     OR COL_4 = '%s'
                ORDER BY
                    COL_1
                ;""".formatted(currentDate);

        String actualSql =
                SqlQuery.select(COL_1, COL_2, COL_3, COL_4)
                        .from(TABLE_1, TABLE_2)
                        .where(COL_1.equalTo("test")
                                .and(COL_2.equalTo(1))
                                .or(COL_3.equalTo(2.0))
                                .or(COL_4.equalTo(currentDate)))
                        .orderBy(COL_1)
                        .build();

        assertEquals(expectedSql, actualSql);
    }

    @Test
    void testSelectQuery_orderByMultipleColumns() {
        Date currentDate = new Date(System.currentTimeMillis());

        String expectedSql = """
                SELECT
                    COL_1,
                    COL_2,
                    COL_3,
                    COL_4
                FROM
                    TABLE_1,
                    TABLE_2
                WHERE
                    COL_1 = 'test'
                    AND COL_2 = 1
                     OR COL_3 = 2.0
                     OR COL_4 = '%s'
                ORDER BY
                    COL_1,
                    COL_2,
                    COL_3 ASC,
                    COL_4 DESC
                ;""".formatted(currentDate);

        String actualSql =
                SqlQuery.select(COL_1, COL_2, COL_3, COL_4)
                        .from(TABLE_1, TABLE_2)
                        .where(COL_1.equalTo("test")
                                .and(COL_2.equalTo(1))
                                .or(COL_3.equalTo(2.0))
                                .or(COL_4.equalTo(currentDate)))
                        .orderBy(COL_1, COL_2, COL_3.asc(), COL_4.desc())
                        .build();

        assertEquals(expectedSql, actualSql);
    }

    @Test
    void testSelectQuery_aliases_comparisonOperators() {
        String expectedSql = """
                SELECT
                    T_1.COL_1 AS C_1,
                    COL_2,
                    COL_3,
                    COL_4
                FROM
                    TABLE_1 AS T_1,
                    TABLE_2 AS T_2
                WHERE
                    NOT T_1.COL_1 = 'test'
                    AND NOT COL_2 = 1
                     OR COL_3 = 2.0
                    AND T_1.COL_1 != 'test'
                    AND T_1.COL_1 > 'test'
                    AND T_1.COL_1 >= 'test'
                    AND T_1.COL_1 < 'test'
                    AND T_1.COL_1 <= 'test'
                    AND T_1.COL_1 BETWEEN 'test1' AND 'test2'
                    AND T_1.COL_1 NOT BETWEEN 'test1' AND 'test2'
                    AND T_1.COL_1 LIKE '%test%'
                    AND T_1.COL_1 NOT LIKE '%test'
                    AND T_1.COL_1 IN ('test1')
                    AND T_1.COL_1 IN ('test1', 'test2', 'test3')
                    AND T_1.COL_1 NOT IN ('test1')
                    AND T_1.COL_1 NOT IN ('test1', 'test2', 'test3')
                    AND T_1.COL_1 IS NULL
                    AND T_1.COL_1 IS NOT NULL
                GROUP BY
                    T_1.COL_1,
                    COL_2
                ORDER BY
                    T_1.COL_1,
                    COL_2,
                    COL_3 ASC,
                    COL_4 DESC
                ;""";

        String actualSql =
                SqlQuery.select(C_1, COL_2, COL_3, COL_4)
                        .from(T_1, T_2)
                        .where(C_1.equalTo("test").negate()
                                .and(COL_2.equalTo(1).negate())
                                .or(COL_3.equalTo(2.0))
                                .and(C_1.notEqualTo("test"))
                                .and(C_1.greaterThan("test"))
                                .and(C_1.greaterThanOrEqualTo("test"))
                                .and(C_1.lessThan("test"))
                                .and(C_1.lessThanOrEqualTo("test"))
                                .and(C_1.between("test1", "test2"))
                                .and(C_1.notBetween("test1", "test2"))
                                .and(C_1.like("%test%"))
                                .and(C_1.notLike("%test"))
                                .and(C_1.in("test1"))
                                .and(C_1.in("test1", "test2", "test3"))
                                .and(C_1.notIn("test1"))
                                .and(C_1.notIn("test1", "test2", "test3"))
                                .and(C_1.isNull())
                                .and(C_1.isNotNull())
                        )
                        .groupBy(C_1, COL_2)
                        .orderBy(C_1, COL_2, COL_3.asc(), COL_4.desc())
                        .build();

        assertEquals(expectedSql, actualSql);
    }
}
