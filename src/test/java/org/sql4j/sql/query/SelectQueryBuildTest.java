package org.sql4j.sql.query;

/*-
 * ==============================LICENSE_START==============================
 * io.github.kumaryoge:sql4j
 * --
 * Copyright (C) 2025 io.github.kumaryoge
 * --
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
 * ===============================LICENSE_END===============================
 */

import org.junit.jupiter.api.Test;
import org.sql4j.sql.query.SelectQuery.ExecutableSelectQuery;

import java.sql.Date;

import static org.sql4j.sql.query.Column.ALL;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SelectQueryBuildTest {
    private static final Table TABLE_1 = Table.forName("TABLE_1");
    private static final Table TABLE_2 = Table.forName("TABLE_2");
    private static final Column<String> COL_1 = Column.forName("COL_1");
    private static final Column<Integer> COL_2 = Column.forName("COL_2");
    private static final Column<Double> COL_3 = Column.forName("COL_3");
    private static final Column<Date> COL_4 = Column.forName("COL_4");

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
                """;

        ExecutableSelectQuery query =
                SqlQuery.select(COL_1)
                        .from(TABLE_1);

        assertEquals(expectedSql, query.sql());
        assertEquals(0, query.params().size());
    }

    @Test
    void testSelectQuery_multipleColumns_singleTable() {
        String expectedSql = """
                SELECT
                    COL_1,
                    COL_2
                FROM
                    TABLE_1
                """;

        ExecutableSelectQuery query =
                SqlQuery.select(COL_1, COL_2)
                        .from(TABLE_1);

        assertEquals(expectedSql, query.sql());
        assertEquals(0, query.params().size());
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
                """;

        ExecutableSelectQuery query =
                SqlQuery.select(COL_1, COL_2, COL_3, COL_4)
                        .from(TABLE_1, TABLE_2);

        assertEquals(expectedSql, query.sql());
        assertEquals(0, query.params().size());
    }

    @Test
    void testSelectQuery_allColumns_singleTable() {
        String expectedSql = """
                SELECT
                    *
                FROM
                    TABLE_1
                """;

        ExecutableSelectQuery query =
                SqlQuery.select(ALL)
                        .from(TABLE_1);

        assertEquals(expectedSql, query.sql());
        assertEquals(0, query.params().size());
    }

    @Test
    void testSelectQuery_allColumns_singleTable_count() {
        String expectedSql = """
                SELECT
                    COUNT(*)
                FROM
                    TABLE_1
                """;

        ExecutableSelectQuery query =
                SqlQuery.select(ALL.count())
                        .from(TABLE_1);

        assertEquals(expectedSql, query.sql());
        assertEquals(0, query.params().size());
    }

    @Test
    void testSelectQuery_singleColumn_singleTable_count() {
        String expectedSql = """
                SELECT
                    COUNT(COL_1)
                FROM
                    TABLE_1
                """;

        ExecutableSelectQuery query =
                SqlQuery.select(COL_1.count())
                        .from(TABLE_1);

        assertEquals(expectedSql, query.sql());
        assertEquals(0, query.params().size());
    }

    @Test
    void testSelectQuery_singleColumn_singleTable_distinct() {
        String expectedSql = """
                SELECT
                    DISTINCT COL_1
                FROM
                    TABLE_1
                """;

        ExecutableSelectQuery query =
                SqlQuery.select(COL_1.distinct())
                        .from(TABLE_1);

        assertEquals(expectedSql, query.sql());
        assertEquals(0, query.params().size());
    }

    @Test
    void testSelectQuery_singleColumn_singleTable_distinct2() {
        String expectedSql = """
                SELECT
                    DISTINCT COL_2
                FROM
                    TABLE_1
                """;

        ExecutableSelectQuery query =
                SqlQuery.select(COL_2.distinct())
                        .from(TABLE_1);

        assertEquals(expectedSql, query.sql());
        assertEquals(0, query.params().size());
    }

    @Test
    void testSelectQuery_singleColumn_singleTable_countDistinct() {
        String expectedSql = """
                SELECT
                    COUNT(DISTINCT COL_2)
                FROM
                    TABLE_1
                """;

        ExecutableSelectQuery query =
                SqlQuery.select(COL_2.countDistinct())
                        .from(TABLE_1);

        assertEquals(expectedSql, query.sql());
        assertEquals(0, query.params().size());
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
                    COL_1 = ?
                """;

        ExecutableSelectQuery query =
                SqlQuery.select(COL_1, COL_2, COL_3, COL_4)
                        .from(TABLE_1, TABLE_2)
                        .where(COL_1.equalTo("test"));

        assertEquals(expectedSql, query.sql());
        assertEquals(1, query.params().size());
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
                    COL_1 = ?
                    AND COL_2 = ?
                     OR COL_3 = ?
                     OR COL_4 = ?
                """;

        ExecutableSelectQuery query =
                SqlQuery.select(COL_1, COL_2, COL_3, COL_4)
                        .from(TABLE_1, TABLE_2)
                        .where(COL_1.equalTo("test")
                                .and(COL_2.equalTo(1))
                                .or(COL_3.equalTo(2.0))
                                .or(COL_4.equalTo(currentDate)));

        assertEquals(expectedSql, query.sql());
        assertEquals(4, query.params().size());
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
                    COL_1 = ?
                    AND COL_2 = ?
                     OR COL_3 = ?
                     OR COL_4 = ?
                GROUP BY
                    COL_1
                """;

        ExecutableSelectQuery query =
                SqlQuery.select(COL_1, COL_2, COL_3, COL_4)
                        .from(TABLE_1, TABLE_2)
                        .where(COL_1.equalTo("test")
                                .and(COL_2.equalTo(1))
                                .or(COL_3.equalTo(2.0))
                                .or(COL_4.equalTo(currentDate)))
                        .groupBy(COL_1);

        assertEquals(expectedSql, query.sql());
        assertEquals(4, query.params().size());
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
                    COL_1 = ?
                    AND COL_2 = ?
                     OR COL_3 = ?
                     OR COL_4 = ?
                GROUP BY
                    COL_1,
                    COL_2
                """;

        ExecutableSelectQuery query =
                SqlQuery.select(COL_1, COL_2, COL_3, COL_4)
                        .from(TABLE_1, TABLE_2)
                        .where(COL_1.equalTo("test")
                                .and(COL_2.equalTo(1))
                                .or(COL_3.equalTo(2.0))
                                .or(COL_4.equalTo(currentDate)))
                        .groupBy(COL_1, COL_2);

        assertEquals(expectedSql, query.sql());
        assertEquals(4, query.params().size());
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
                    COL_1 = ?
                    AND COL_2 = ?
                     OR COL_3 = ?
                     OR COL_4 = ?
                ORDER BY
                    COL_1
                """;

        ExecutableSelectQuery query =
                SqlQuery.select(COL_1, COL_2, COL_3, COL_4)
                        .from(TABLE_1, TABLE_2)
                        .where(COL_1.equalTo("test")
                                .and(COL_2.equalTo(1))
                                .or(COL_3.equalTo(2.0))
                                .or(COL_4.equalTo(currentDate)))
                        .orderBy(COL_1);

        assertEquals(expectedSql, query.sql());
        assertEquals(4, query.params().size());
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
                    COL_1 = ?
                    AND COL_2 = ?
                     OR COL_3 = ?
                     OR COL_4 = ?
                ORDER BY
                    COL_1,
                    COL_2,
                    COL_3 ASC,
                    COL_4 DESC
                """;

        ExecutableSelectQuery query =
                SqlQuery.select(COL_1, COL_2, COL_3, COL_4)
                        .from(TABLE_1, TABLE_2)
                        .where(COL_1.equalTo("test")
                                .and(COL_2.equalTo(1))
                                .or(COL_3.equalTo(2.0))
                                .or(COL_4.equalTo(currentDate)))
                        .orderBy(COL_1, COL_2, COL_3.asc(), COL_4.desc());

        assertEquals(expectedSql, query.sql());
        assertEquals(4, query.params().size());
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
                    NOT T_1.COL_1 = ?
                    AND NOT COL_2 = ?
                     OR COL_3 = ?
                    AND T_1.COL_1 != ?
                    AND T_1.COL_1 > ?
                    AND T_1.COL_1 >= ?
                    AND T_1.COL_1 < ?
                    AND T_1.COL_1 <= ?
                    AND T_1.COL_1 BETWEEN ? AND ?
                    AND T_1.COL_1 NOT BETWEEN ? AND ?
                    AND T_1.COL_1 LIKE ?
                    AND T_1.COL_1 NOT LIKE ?
                    AND T_1.COL_1 IN (?)
                    AND T_1.COL_1 IN (?, ?, ?)
                    AND T_1.COL_1 NOT IN (?)
                    AND T_1.COL_1 NOT IN (?, ?, ?)
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
                """;

        ExecutableSelectQuery query =
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
                        .orderBy(C_1, COL_2, COL_3.asc(), COL_4.desc());

        assertEquals(expectedSql, query.sql());
        assertEquals(22, query.params().size());
    }

    @Test
    void testSelectQuery_compositeConditions() {
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
                    NOT COL_1 = ?
                    AND (NOT COL_2 = ?
                     OR NOT (NOT COL_3 = ?
                     OR NOT COL_4 = ?))
                """;

        ExecutableSelectQuery query =
                SqlQuery.select(COL_1, COL_2, COL_3, COL_4)
                        .from(TABLE_1, TABLE_2)
                        .where(COL_1.equalTo("test").negate()
                                .and(COL_2.equalTo(1).negate()
                                        .or(COL_3.equalTo(2.0).negate()
                                                .or(COL_4.equalTo(currentDate).negate())
                                                .negate())));

        assertEquals(expectedSql, query.sql());
        assertEquals(4, query.params().size());
    }
}
