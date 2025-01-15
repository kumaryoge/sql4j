package org.sql4j.sql.query;

import lombok.NonNull;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SelectQuery {
    private final Context context = new Context();

    SelectQuery(@NonNull Column<?> column, @NonNull Column<?>[] columns) {
        Arrays.stream(columns).forEach(Objects::requireNonNull);
        context.sqlBuilder.append("SELECT\n    ")
                .append(Stream.concat(Stream.of(column), Arrays.stream(columns))
                        .map(c -> c.getName() + Optional.ofNullable(c.getAlias()).map(alias -> " AS " + alias).orElse(""))
                        .collect(Collectors.joining(",\n    ")))
                .append("\n");
    }

    public ConditionableSelectQuery from(@NonNull Table table, @NonNull Table... tables) {
        Arrays.stream(tables).forEach(Objects::requireNonNull);
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

    public static class ConditionableSelectQuery extends GroupableSelectQuery {

        private ConditionableSelectQuery(Context context) {
            super(context);
        }

        public GroupableSelectQuery where(@NonNull Filter filter) {
            context.sqlBuilder.append("WHERE\n    ")
                    .append(filter.getCondition())
                    .append("\n");
            context.params.addAll(filter.getParams());
            return this;
        }
    }

    public static class GroupableSelectQuery extends OrderableSelectQuery {

        private GroupableSelectQuery(Context context) {
            super(context);
        }

        public OrderableSelectQuery groupBy(@NonNull Column<?> column, @NonNull Column<?>... columns) {
            Arrays.stream(columns).forEach(Objects::requireNonNull);
            context.sqlBuilder.append("GROUP BY\n    ")
                    .append(Stream.concat(Stream.of(column), Arrays.stream(columns))
                            .map(Column::getName)
                            .collect(Collectors.joining(",\n    ")))
                    .append("\n");
            return this;
        }
    }

    public static class OrderableSelectQuery extends ExecutableSelectQuery {

        private OrderableSelectQuery(Context context) {
            super(context);
        }

        public ExecutableSelectQuery orderBy(@NonNull Column<?> column, @NonNull Column<?>... columns) {
            Arrays.stream(columns).forEach(Objects::requireNonNull);
            context.sqlBuilder.append("ORDER BY\n    ")
                    .append(Stream.concat(Stream.of(column), Arrays.stream(columns))
                            .map(c -> c.getName() + (c.getOrder() != null ? (" " + c.getOrder()) : ""))
                            .collect(Collectors.joining(",\n    ")))
                    .append("\n");
            return this;
        }
    }

    public static class ExecutableSelectQuery {
        protected final Context context;

        private ExecutableSelectQuery(Context context) {
            this.context = context;
        }

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

        String sql() {
            return context.sqlBuilder.toString() + ";";
        }

        List<Object> params() {
            return context.params;
        }
    }
}
