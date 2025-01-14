package org.sql4j.sql.query;

public class SqlQuery {

    public static SelectQuery select(Column<?> column, Column<?>... columns) {
        return new SelectQuery(column, columns);
    }
}
