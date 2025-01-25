package org.sql4j.sql.query;

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
