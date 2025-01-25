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

import lombok.NonNull;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class used to build and execute sql {@code SELECT} queries
 */

public class SelectQuery {
    private final Context context = new Context();

    SelectQuery(@NonNull Column<?> column, @NonNull Column<?>[] columns) {
        Utils.requireNonNulls(columns);
        context.sqlBuilder.append("SELECT\n    ")
                .append(Stream.concat(Stream.of(column), Arrays.stream(columns))
                        .map(c -> c.getName() + Optional.ofNullable(c.getAlias()).map(alias -> " AS " + alias).orElse(""))
                        .collect(Collectors.joining(",\n    ")))
                .append("\n");
    }

    /**
     * @param table a table to select columns from
     * @param tables other tables to select columns from
     * @return a {@link ConditionableSelectQuery} with the given tables in {@code FROM} clause e.g. {@code SELECT * FROM table, <tables separated by comma> ...}
     */
    public ConditionableSelectQuery from(@NonNull Table table, @NonNull Table... tables) {
        Utils.requireNonNulls(tables);
        context.sqlBuilder.append("FROM\n    ")
                .append(Stream.concat(Stream.of(table), Arrays.stream(tables))
                        .map(t -> t.getName() + Optional.ofNullable(t.getAlias()).map(alias -> " AS " + alias).orElse(""))
                        .collect(Collectors.joining(",\n    ")))
                .append("\n");
        return new ConditionableSelectQuery(context);
    }

    private static class Context {
        private final StringBuilder sqlBuilder = new StringBuilder();
        private final List<Object> params = new ArrayList<>();
    }

    /**
     * Class to represent a {@code SELECT} query that can be used to add {@code WHERE} clause in the query
     */
    public static class ConditionableSelectQuery extends GroupableSelectQuery {

        private ConditionableSelectQuery(Context context) {
            super(context);
        }

        /**
         * @param filter a {@link Filter} that contains the condition used in {@code WHERE} clause
         * @return a {@link GroupableSelectQuery} that can be used to add {@code GROUP BY} clause in the query
         */
        public GroupableSelectQuery where(@NonNull Filter filter) {
            context.sqlBuilder.append("WHERE\n    ")
                    .append(filter.getCondition())
                    .append("\n");
            context.params.addAll(filter.getParams());
            return this;
        }
    }

    /**
     * Class to represent a {@code SELECT} query that can be used to add {@code GROUP BY} clause in the query
     */
    public static class GroupableSelectQuery extends OrderableSelectQuery {

        private GroupableSelectQuery(Context context) {
            super(context);
        }

        /**
         * @param column a column for grouping the results
         * @param columns other columns for grouping the results
         * @return an {@link OrderableSelectQuery} that can be used to add {@code ORDER BY} clause in the query
         */
        public OrderableSelectQuery groupBy(@NonNull Column<?> column, @NonNull Column<?>... columns) {
            Utils.requireNonNulls(columns);
            context.sqlBuilder.append("GROUP BY\n    ")
                    .append(Stream.concat(Stream.of(column), Arrays.stream(columns))
                            .map(Column::getName)
                            .collect(Collectors.joining(",\n    ")))
                    .append("\n");
            return this;
        }
    }

    /**
     * Class to represent a {@code SELECT} query that can be used to add {@code ORDER BY} clause in the query
     */
    public static class OrderableSelectQuery extends ExecutableSelectQuery {

        private OrderableSelectQuery(Context context) {
            super(context);
        }

        /**
         * @param column a column for ordering the results
         * @param columns other columns for ordering the results
         * @return an {@link ExecutableSelectQuery} that is ready for execution
         */
        public ExecutableSelectQuery orderBy(@NonNull Column<?> column, @NonNull Column<?>... columns) {
            Utils.requireNonNulls(columns);
            context.sqlBuilder.append("ORDER BY\n    ")
                    .append(Stream.concat(Stream.of(column), Arrays.stream(columns))
                            .map(c -> c.getName() + (c.getOrder() != null ? (" " + c.getOrder()) : ""))
                            .collect(Collectors.joining(",\n    ")))
                    .append("\n");
            return this;
        }
    }

    /**
     * Class to represent a {@code SELECT} query that is ready for execution
     */
    public static class ExecutableSelectQuery {
        protected final Context context;

        private ExecutableSelectQuery(Context context) {
            this.context = context;
        }

        /**
         * @param con a {@link java.sql.Connection} object that can be created via {@link java.sql.DriverManager#getConnection(String, String, String)}
         * @param resultSetMapper a {@link ResultSetMapper} to map a {@link java.sql.ResultSet} to an object of type {@code <T>}
         * @param <T> a java class type to represent a row/record in the results of the {@code SELECT} query
         * @return a list of objects of type {@code <T>} representing the results of the {@code SELECT} query
         * @throws SQLException if a database access error occurs
         */
        public <T> List<T> execute(Connection con, ResultSetMapper<T> resultSetMapper) throws SQLException {
            List<T> results = new ArrayList<>();

            try (PreparedStatement stmt = con.prepareStatement(sql())) {
                for (int i = 0; i < context.params.size(); ++i) {
                    stmt.setObject(i + 1, context.params.get(i));
                }

                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    results.add(resultSetMapper.map(rs));
                }
            }

            return results;
        }

        /**
         * @return the sql statement used to execute the sql query, callers may want to log it for debugging/information
         */
        public String sql() {
            return context.sqlBuilder.toString();
        }

        /**
         * @return the values of parameters used in sql statement, callers may want to log it for debugging/information
         */
        public List<Object> params() {
            return context.params;
        }
    }
}
