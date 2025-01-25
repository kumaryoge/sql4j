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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class used to build and execute sql {@code INSERT} queries
 */

public class InsertQuery {
    private final Context context = new Context();

    InsertQuery() {
        context.sqlBuilder.append("INSERT\n");
    }

    /**
     * @param table a table to insert rows into
     * @return a {@link ValuableInsertQuery} with the given {@code table} in {@code INTO} clause e.g. {@code INSERT INTO table ...}
     */
    public ValuableInsertQuery into(@NonNull Table table) {
        context.sqlBuilder.append("INTO\n    ")
                .append(table.getName())
                .append("\n    ");
        return new ValuableInsertQuery(context);
    }

    private static class Context {
        private final StringBuilder sqlBuilder = new StringBuilder();
        private final List<ColumnValue> valueParams = new ArrayList<>();
    }

    /**
     * Class to represent an {@code INSERT} query that can be used to add {@code VALUES} clause in the query
     */
    public static class ValuableInsertQuery {
        private final Context context;

        private ValuableInsertQuery(Context context) {
            this.context = context;
        }

        /**
         * @param colValue a {@link ColumnValue} to be inserted
         * @param colValues other {@link ColumnValue}s to be inserted
         * @return an {@link ExecutableInsertQuery} that is ready for execution
         */
        public ExecutableInsertQuery values(@NonNull ColumnValue colValue, @NonNull ColumnValue... colValues) {
            Utils.requireNonNulls(colValues);
            context.sqlBuilder.append("(")
                    .append(Stream.concat(Stream.of(colValue), Arrays.stream(colValues))
                            .map(ColumnValue::getName)
                            .collect(Collectors.joining(", ")))
                    .append(")\n");
            context.sqlBuilder.append("VALUES\n    (?")
                    .append(", ?".repeat(colValues.length))
                    .append(")\n");
            context.valueParams.addAll(Stream.concat(Stream.of(colValue), Arrays.stream(colValues)).toList());
            return new ExecutableInsertQuery(context);
        }
    }

    /**
     * Class to represent an {@code INSERT} query that is ready for execution
     */
    public static class ExecutableInsertQuery {
        protected final Context context;

        private ExecutableInsertQuery(Context context) {
            this.context = context;
        }

        /**
         * @param con a {@link java.sql.Connection} object that can be created via {@link java.sql.DriverManager#getConnection(String, String, String)}
         * @return number of inserted rows in the table
         * @throws SQLException if a database access error occurs
         */
        public int execute(Connection con) throws SQLException {
            try (PreparedStatement stmt = con.prepareStatement(sql())) {
                for (int i = 0; i < context.valueParams.size(); ++i) {
                    ColumnValue colValue = context.valueParams.get(i);

                    if (colValue.getValue() != null) {
                        stmt.setObject(i + 1, colValue.getValue());
                    } else {
                        stmt.setNull(i + 1, colValue.getSqlType());
                    }
                }

                return stmt.executeUpdate();
            }
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
            return context.valueParams.stream().map(ColumnValue::getValue).toList();
        }
    }
}
