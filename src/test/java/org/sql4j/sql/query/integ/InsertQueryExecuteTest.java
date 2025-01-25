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
import org.sql4j.sql.query.Column;
import org.sql4j.sql.query.SqlQuery;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class InsertQueryExecuteTest extends SqlQueryExecuteTestBase {

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testInsertQuery(Connection connection) throws SQLException {
        int rowCount =
                SqlQuery.insert()
                        .into(TABLE_1)
                        .values(COL_1.value("test"), COL_2.value(1))
                        .execute(connection);

        assertEquals(1, rowCount);
        assertEquals(Table1Row.builder().col1("test").col2(1).build(),
                SqlQuery.select(Column.ALL)
                        .from(TABLE_1)
                        .execute(connection, rs -> Table1Row.builder()
                                .col1(rs.getString(COL_1.getName()))
                                .col2(rs.getInt(COL_2.getName()))
                                .build())
                        .getFirst());

        rowCount =
                SqlQuery.delete()
                        .from(TABLE_1)
                        .where(COL_1.equalTo("test"))
                        .execute(connection);

        assertEquals(1, rowCount);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @FieldSource("CONNECTIONS")
    void testInsertQuery_setNull(Connection connection) throws SQLException {
        Column<Double> COL_3 = Column.forNameAndType("COL_3", Types.DOUBLE);

        int rowCount =
                SqlQuery.insert()
                        .into(TABLE_1)
                        .values(COL_1.value("test"), COL_2.value(1), COL_3.value(null))
                        .execute(connection);

        assertEquals(1, rowCount);
        assertEquals(Table1Row.builder().col1("test").col2(1).col3(null).build(),
                SqlQuery.select(Column.ALL)
                        .from(TABLE_1)
                        .execute(connection, rs -> Table1Row.builder()
                                .col1(rs.getString(COL_1.getName()))
                                .col2(rs.getInt(COL_2.getName()))
                                .col3(rs.getObject(COL_3.getName(), Double.class))
                                .build())
                        .getFirst());

        rowCount =
                SqlQuery.delete()
                        .from(TABLE_1)
                        .where(COL_1.equalTo("test"))
                        .execute(connection);

        assertEquals(1, rowCount);
    }
}
