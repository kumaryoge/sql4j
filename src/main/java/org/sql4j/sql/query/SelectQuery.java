package org.sql4j.sql.query;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SelectQuery {
    private final StringBuilder sqlBuilder = new StringBuilder();

    public SelectQuery(Column<?> column, Column<?>[] columns) {
        sqlBuilder.append("SELECT\n    ")
                .append(Stream.concat(Stream.of(column), Arrays.stream(columns))
                        .map(c -> c.getName() + Optional.ofNullable(c.getAlias()).map(alias -> " AS " + alias).orElse(""))
                        .collect(Collectors.joining(",\n    ")))
                .append("\n");
    }

    public ConditionableSelectQuery from(Table table, Table... tables) {
        sqlBuilder.append("FROM\n    ")
                .append(Stream.concat(Stream.of(table), Arrays.stream(tables))
                        .map(t -> t.getName() + Optional.ofNullable(t.getAlias()).map(alias -> " AS " + alias).orElse(""))
                        .collect(Collectors.joining(",\n    ")))
                .append("\n");
        return new ConditionableSelectQuery(sqlBuilder);
    }

    public static class ConditionableSelectQuery extends GroupableSelectQuery {

        public ConditionableSelectQuery(StringBuilder sqlBuilder) {
            super(sqlBuilder);
        }

        public GroupableSelectQuery where(Filter filter) {
            sqlBuilder.append("WHERE\n    ")
                    .append(filter.getCondition())
                    .append("\n");
            return this;
        }
    }

    public static class GroupableSelectQuery extends OrderableSelectQuery {

        public GroupableSelectQuery(StringBuilder sqlBuilder) {
            super(sqlBuilder);
        }

        public OrderableSelectQuery groupBy(Column<?> column, Column<?>... columns) {
            sqlBuilder.append("GROUP BY\n    ")
                    .append(Stream.concat(Stream.of(column), Arrays.stream(columns))
                            .map(Column::getName)
                            .collect(Collectors.joining(",\n    ")))
                    .append("\n");
            return this;
        }
    }

    public static class OrderableSelectQuery extends ExecutableSelectQuery {

        public OrderableSelectQuery(StringBuilder sqlBuilder) {
            super(sqlBuilder);
        }

        public ExecutableSelectQuery orderBy(Column<?> column, Column<?>... columns) {
            sqlBuilder.append("ORDER BY\n    ")
                    .append(Stream.concat(Stream.of(column), Arrays.stream(columns))
                            .map(c -> c.getName() + (c.getOrder() != null ? (" " + c.getOrder()) : ""))
                            .collect(Collectors.joining(",\n    ")))
                    .append("\n");
            return this;
        }
    }

    public static class ExecutableSelectQuery {
        protected final StringBuilder sqlBuilder;

        public ExecutableSelectQuery(StringBuilder sqlBuilder) {
            this.sqlBuilder = sqlBuilder;
        }

        public String build() {
            return sqlBuilder.toString() + ";";
        }

        public <T> List<T> execute(Connection con, ResultSetMapper<T> resultSetMapper) throws SQLException {
            List<T> results = new ArrayList<>();

            try (Statement stmt = con.createStatement()) {
                String query = build();
                ResultSet rs = stmt.executeQuery(query);

                while (rs.next()) {
                    results.add(resultSetMapper.map(rs));
                }
            }

            return results;
        }
    }
}
