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
 * Class used to build and execute sql {@code UPDATE} queries
 */

public class UpdateQuery {
    private final Context context = new Context();

    UpdateQuery(@NonNull Table table) {
        context.sqlBuilder.append("UPDATE\n    ")
                .append(table.getName())
                .append("\n");
    }

    /**
     * @param colValue a {@link ColumnValue} to be set
     * @param colValues other {@link ColumnValue}s to be set
     * @return a {@link ConditionableUpdateQuery} with the given column values in {@code SET} clause
     */
    public ConditionableUpdateQuery set(@NonNull ColumnValue colValue, @NonNull ColumnValue... colValues) {
        Utils.requireNonNulls(colValues);
        context.sqlBuilder.append("SET\n    ")
                .append(Stream.concat(Stream.of(colValue), Arrays.stream(colValues))
                        .map(cv -> cv.getName() + " = ?")
                        .collect(Collectors.joining(",\n    ")))
                .append("\n");
        context.setParams.addAll(Stream.concat(Stream.of(colValue), Arrays.stream(colValues)).toList());
        return new ConditionableUpdateQuery(context);
    }

    private static class Context {
        private final StringBuilder sqlBuilder = new StringBuilder();
        private final List<ColumnValue> setParams = new ArrayList<>();
        private final List<Object> filterParams = new ArrayList<>();
    }

    /**
     * Class to represent an {@code UPDATE} query that can be used to add {@code WHERE} clause in the query
     */
    public static class ConditionableUpdateQuery extends ExecutableUpdateQuery {

        private ConditionableUpdateQuery(Context context) {
            super(context);
        }

        /**
         * @param filter a {@link Filter} that contains the condition used in {@code WHERE} clause
         * @return an {@link ExecutableUpdateQuery} that is ready for execution
         */
        public ExecutableUpdateQuery where(@NonNull Filter filter) {
            context.sqlBuilder.append("WHERE\n    ")
                    .append(filter.getCondition())
                    .append("\n");
            context.filterParams.addAll(filter.getParams());
            return this;
        }
    }

    /**
     * Class to represent an {@code UPDATE} query that is ready for execution
     */
    public static class ExecutableUpdateQuery {
        protected final Context context;

        private ExecutableUpdateQuery(Context context) {
            this.context = context;
        }

        /**
         * @param con a {@link java.sql.Connection} object that can be created via {@link java.sql.DriverManager#getConnection(String, String, String)}
         * @return number of updated rows in the table
         * @throws SQLException if a database access error occurs
         */
        public int execute(Connection con) throws SQLException {
            try (PreparedStatement stmt = con.prepareStatement(sql())) {
                for (int i = 0; i < context.setParams.size(); ++i) {
                    ColumnValue colValue = context.setParams.get(i);

                    if (colValue.getValue() != null) {
                        stmt.setObject(i + 1, colValue.getValue());
                    } else {
                        stmt.setNull(i + 1, colValue.getSqlType());
                    }
                }

                for (int i = 0; i < context.filterParams.size(); ++i) {
                    stmt.setObject(context.setParams.size() + i + 1, context.filterParams.get(i));
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
            return Stream.concat(context.setParams.stream().map(ColumnValue::getValue), context.filterParams.stream()).toList();
        }
    }
}
