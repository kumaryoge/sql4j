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
import org.sql4j.sql.query.UpdateQuery.ExecutableUpdateQuery;

import java.sql.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UpdateQueryBuildTest {
    private static final Table TABLE_1 = Table.forName("TABLE_1");
    private static final Column<String> COL_1 = Column.forName("COL_1");
    private static final Column<Integer> COL_2 = Column.forName("COL_2");
    private static final Column<Double> COL_3 = Column.forName("COL_3");
    private static final Column<Date> COL_4 = Column.forName("COL_4");

    @Test
    void testUpdateQuery_singleColumn_allRecords() {
        String expectedSql = """
                UPDATE
                    TABLE_1
                SET
                    COL_1 = ?
                """;

        ExecutableUpdateQuery query =
                SqlQuery.update(TABLE_1)
                        .set(COL_1.value("test"));

        assertEquals(expectedSql, query.sql());
        assertEquals(1, query.params().size());
    }

    @Test
    void testUpdateQuery_singleColumn_selectedRecords_singleCondition() {
        String expectedSql = """
                UPDATE
                    TABLE_1
                SET
                    COL_1 = ?
                WHERE
                    COL_1 = ?
                """;

        ExecutableUpdateQuery query =
                SqlQuery.update(TABLE_1)
                        .set(COL_1.value("test"))
                        .where(COL_1.equalTo("test"));

        assertEquals(expectedSql, query.sql());
        assertEquals(2, query.params().size());
    }

    @Test
    void testUpdateQuery_singleColumn_selectedRecords_multipleConditions() {
        Date currentDate = new Date(System.currentTimeMillis());

        String expectedSql = """
                UPDATE
                    TABLE_1
                SET
                    COL_1 = ?
                WHERE
                    COL_1 = ?
                    AND COL_2 = ?
                     OR COL_3 = ?
                     OR COL_4 = ?
                """;

        ExecutableUpdateQuery query =
                SqlQuery.update(TABLE_1)
                        .set(COL_1.value("test"))
                        .where(COL_1.equalTo("test")
                                .and(COL_2.equalTo(1))
                                .or(COL_3.equalTo(2.0))
                                .or(COL_4.equalTo(currentDate)));

        assertEquals(expectedSql, query.sql());
        assertEquals(5, query.params().size());
    }

    @Test
    void testUpdateQuery_multipleColumns_allRecords() {
        Date currentDate = new Date(System.currentTimeMillis());

        String expectedSql = """
                UPDATE
                    TABLE_1
                SET
                    COL_1 = ?,
                    COL_2 = ?,
                    COL_3 = ?,
                    COL_4 = ?
                """;

        ExecutableUpdateQuery query =
                SqlQuery.update(TABLE_1)
                        .set(COL_1.value("test"),
                                COL_2.value(1),
                                COL_3.value(1.0),
                                COL_4.value(currentDate));

        assertEquals(expectedSql, query.sql());
        assertEquals(4, query.params().size());
    }

    @Test
    void testUpdateQuery_multipleColumns_selectedRecords_singleCondition() {
        Date currentDate = new Date(System.currentTimeMillis());

        String expectedSql = """
                UPDATE
                    TABLE_1
                SET
                    COL_1 = ?,
                    COL_2 = ?,
                    COL_3 = ?,
                    COL_4 = ?
                WHERE
                    COL_1 = ?
                """;

        ExecutableUpdateQuery query =
                SqlQuery.update(TABLE_1)
                        .set(COL_1.value("test"),
                                COL_2.value(1),
                                COL_3.value(1.0),
                                COL_4.value(currentDate))
                        .where(COL_1.equalTo("test"));

        assertEquals(expectedSql, query.sql());
        assertEquals(5, query.params().size());
    }

    @Test
    void testUpdateQuery_multipleColumns_selectedRecords_multipleConditions() {
        Date currentDate = new Date(System.currentTimeMillis());

        String expectedSql = """
                UPDATE
                    TABLE_1
                SET
                    COL_1 = ?,
                    COL_2 = ?,
                    COL_3 = ?,
                    COL_4 = ?
                WHERE
                    COL_1 = ?
                    AND COL_2 = ?
                     OR COL_3 = ?
                     OR COL_4 = ?
                """;

        ExecutableUpdateQuery query =
                SqlQuery.update(TABLE_1)
                        .set(COL_1.value("test"),
                                COL_2.value(1),
                                COL_3.value(1.0),
                                COL_4.value(currentDate))
                        .where(COL_1.equalTo("test")
                                .and(COL_2.equalTo(1))
                                .or(COL_3.equalTo(2.0))
                                .or(COL_4.equalTo(currentDate)));

        assertEquals(expectedSql, query.sql());
        assertEquals(8, query.params().size());
    }
}
