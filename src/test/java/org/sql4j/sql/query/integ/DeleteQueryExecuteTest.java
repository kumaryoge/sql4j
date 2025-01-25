package org.sql4j.sql.query.integ;

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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;
import org.sql4j.sql.query.SqlQuery;

import java.sql.*;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DeleteQueryExecuteTest extends SqlQueryExecuteTestBase {

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testDeleteQuery_allRecords(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

        int rowCount =
                SqlQuery.delete()
                        .from(TABLE_1)
                        .execute(connection);

        assertEquals(RECORDS.size(), rowCount);

        deleteRecordsFromTable1(RECORDS, connection);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testDeleteQuery_selectedRecords_singleCondition(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

        int rowCount =
                SqlQuery.delete()
                        .from(TABLE_1)
                        .where(COL_1.equalTo("test1"))
                        .execute(connection);

        assertEquals(RECORDS.stream().filter(record -> Objects.equals(record.col1(), "test1")).count(), rowCount);

        deleteRecordsFromTable1(RECORDS, connection);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testDeleteQuery_selectedRecords_multipleConditions(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

        int rowCount =
                SqlQuery.delete()
                        .from(TABLE_1)
                        .where(COL_1.equalTo("test1")
                                .and(COL_2.equalTo(10))
                                .or(COL_3.equalTo(1.4))
                                .or(COL_4.equalTo(Date.valueOf("2025-01-05"))))
                        .execute(connection);

        assertEquals(RECORDS.stream()
                .filter(record ->
                        Objects.equals(record.col1(), "test1")
                                && Objects.equals(record.col2(), 10)
                                || Objects.equals(record.col3(), 1.4)
                                || Objects.equals(record.col4(), Date.valueOf("2025-01-05")))
                .count(), rowCount);

        deleteRecordsFromTable1(RECORDS, connection);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testDeleteQuery_selectedRecords_multipleConditions2(Connection connection) throws SQLException {
        insertRecordsInTable1(RECORDS, connection);

        int rowCount =
                SqlQuery.delete()
                        .from(TABLE_1)
                        .where(COL_1.equalTo("test1")
                                .and(COL_2.equalTo(10)
                                        .or(COL_3.equalTo(1.4)))
                                .or(COL_4.equalTo(Date.valueOf("2025-01-05"))))
                        .execute(connection);

        assertEquals(RECORDS.stream()
                .filter(record ->
                        Objects.equals(record.col1(), "test1")
                                && (Objects.equals(record.col2(), 10)
                                    || Objects.equals(record.col3(), 1.4))
                                || Objects.equals(record.col4(), Date.valueOf("2025-01-05")))
                .count(), rowCount);

        deleteRecordsFromTable1(RECORDS, connection);
    }
}
