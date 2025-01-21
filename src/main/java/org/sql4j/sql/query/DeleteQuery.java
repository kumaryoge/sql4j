package org.sql4j.sql.query;

import lombok.NonNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DeleteQuery {
    private final Context context = new Context();

    DeleteQuery() {
        context.sqlBuilder.append("DELETE\n");
    }

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

    public static class ConditionableDeleteQuery extends ExecutableDeleteQuery {

        private ConditionableDeleteQuery(Context context) {
            super(context);
        }

        public ExecutableDeleteQuery where(@NonNull Filter filter) {
            context.sqlBuilder.append("WHERE\n    ")
                    .append(filter.getCondition())
                    .append("\n");
            context.params.addAll(filter.getParams());
            return this;
        }
    }

    public static class ExecutableDeleteQuery {
        protected final Context context;

        private ExecutableDeleteQuery(Context context) {
            this.context = context;
        }

        public int execute(Connection con) throws SQLException {
            try (PreparedStatement stmt = con.prepareStatement(sql())) {
                for (int i = 0; i < context.params.size(); ++i) {
                    stmt.setObject(i + 1, context.params.get(i));
                }

                return stmt.executeUpdate();
            }
        }

        String sql() {
            return context.sqlBuilder.toString();
        }

        List<Object> params() {
            return context.params;
        }
    }
}
