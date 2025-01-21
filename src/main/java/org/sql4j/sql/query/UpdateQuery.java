package org.sql4j.sql.query;

import lombok.NonNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UpdateQuery {
    private final Context context = new Context();

    UpdateQuery(@NonNull Table table) {
        context.sqlBuilder.append("UPDATE\n    ")
                .append(table.getName())
                .append("\n");
    }

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

    public static class ConditionableUpdateQuery extends ExecutableUpdateQuery {

        private ConditionableUpdateQuery(Context context) {
            super(context);
        }

        public ExecutableUpdateQuery where(@NonNull Filter filter) {
            context.sqlBuilder.append("WHERE\n    ")
                    .append(filter.getCondition())
                    .append("\n");
            context.filterParams.addAll(filter.getParams());
            return this;
        }
    }

    public static class ExecutableUpdateQuery {
        protected final Context context;

        private ExecutableUpdateQuery(Context context) {
            this.context = context;
        }

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

        String sql() {
            return context.sqlBuilder.toString();
        }

        List<Object> params() {
            return Stream.concat(context.setParams.stream().map(ColumnValue::getValue), context.filterParams.stream()).toList();
        }
    }
}
