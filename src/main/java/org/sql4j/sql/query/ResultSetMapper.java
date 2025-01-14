package org.sql4j.sql.query;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Example usage:
 * <p>
 * 1. When reading a single column (with name "COL_1" and type String) from a database table:
 * <blockquote><pre>
 *     ResultSetMapper<String> resultSetMapper = rs -> rs.getString("COL_1");
 * </pre></blockquote>
 * <p>
 * 2. When reading multiple columns:
 * <blockquote><pre>
 *     ResultSetMapper<Row> resultSetMapper = rs -> Row.builder()
 *                                                     .col1(rs.getString("COL_1"))
 *                                                     .col2(rs.getInt("COL_2"))
 *                                                     .col3(rs.getDouble("COL_3"))
 *                                                     .col4(rs.getDate("COL_4"))
 *                                                     .build();
 * </pre></blockquote>
 * Where {@code Row} is defined as:
 * <blockquote><pre>
 *     import lombok.Getter;
 *     import lombok.Builder;
 *
 *     {@code @Getter}
 *     {@code @Builder}
 *     class Row {
 *         private final String col1;
 *         private final int col2;
 *         private final double col3;
 *         private final Date col4;
 *     }
 * </pre></blockquote>
 *
 * @param <T>
 */

@FunctionalInterface
public interface ResultSetMapper<T> {
    T map(ResultSet rs) throws SQLException;
}
