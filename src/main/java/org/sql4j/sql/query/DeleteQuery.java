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
import java.util.List;

/**
 * Class used to build and execute sql {@code DELETE} queries
 */

public class DeleteQuery {
    private final Context context = new Context();

    DeleteQuery() {
        context.sqlBuilder.append("DELETE\n");
    }

    /**
     * @param table a table to delete rows from
     * @return a {@link ConditionableDeleteQuery} with the given {@code table} in {@code FROM} clause e.g. {@code DELETE FROM table ...}
     */
    public ConditionableDeleteQuery from(@NonNull Table table) {
        context.sqlBuilder.append("FROM\n    ")
                .append(table.getName())
                .append("\n");
        return new ConditionableDeleteQuery(context);
    }

    private static class Context {
        private final StringBuilder sqlBuilder = new StringBuilder();
        private final List<Object> params = new ArrayList<>();
    }

    /**
     * Class to represent a {@code DELETE} query that can be used to add {@code WHERE} clause in the query
     */
    public static class ConditionableDeleteQuery extends ExecutableDeleteQuery {

        private ConditionableDeleteQuery(Context context) {
            super(context);
        }

        /**
         * @param filter a {@link Filter} that contains the condition used in {@code WHERE} clause
         * @return an {@link ExecutableDeleteQuery} that is ready for execution
         */
        public ExecutableDeleteQuery where(@NonNull Filter filter) {
            context.sqlBuilder.append("WHERE\n    ")
                    .append(filter.getCondition())
                    .append("\n");
            context.params.addAll(filter.getParams());
            return this;
        }
    }

    /**
     * Class to represent a {@code DELETE} query that is ready for execution
     */
    public static class ExecutableDeleteQuery {
        protected final Context context;

        private ExecutableDeleteQuery(Context context) {
            this.context = context;
        }

        /**
         * @param con a {@link java.sql.Connection} object that can be created via {@link java.sql.DriverManager#getConnection(String, String, String)}
         * @return number of deleted rows in the table
         * @throws SQLException if a database access error occurs
         */
        public int execute(Connection con) throws SQLException {
            try (PreparedStatement stmt = con.prepareStatement(sql())) {
                for (int i = 0; i < context.params.size(); ++i) {
                    stmt.setObject(i + 1, context.params.get(i));
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
            return context.params;
        }
    }
}
