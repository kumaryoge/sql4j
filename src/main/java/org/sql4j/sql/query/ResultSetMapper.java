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

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Example usage:
 * <p>
 * 1. When reading a single column (with name {@code COL_1} and type {@code String}) from a database table:
 * <blockquote><pre>
 *     ResultSetMapper{@code <String>} resultSetMapper = rs -> rs.getString("COL_1");
 * </pre></blockquote>
 * <p>
 * 2. When reading multiple columns:
 * <blockquote><pre>
 *     ResultSetMapper{@code <Row>} resultSetMapper = rs -> Row.builder()
 *                                                     .col1(rs.getString("COL_1"))
 *                                                     .col2(rs.getInt("COL_2"))
 *                                                     .col3(rs.getDouble("COL_3"))
 *                                                     .col4(rs.getDate("COL_4"))
 *                                                     .build();
 * </pre></blockquote>
 * Where {@code Row} is defined as:
 * <blockquote><pre>
 *     {@code @lombok.Builder}
 *     record Row(String col1, int col2, double col3, Date col4) {}
 * </pre></blockquote>
 *
 * @param <T> a java class type to represent a row/record in the results of a {@code SELECT} query
 */

@FunctionalInterface
public interface ResultSetMapper<T> {
    T map(ResultSet rs) throws SQLException;
}
