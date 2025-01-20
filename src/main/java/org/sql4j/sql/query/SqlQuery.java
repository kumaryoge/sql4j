package org.sql4j.sql.query;

import lombok.NonNull;

public class SqlQuery {

    public static SelectQuery select(@NonNull Column<?> column, @NonNull Column<?>... columns) {
        Utils.requireNonNulls(columns);
        return new SelectQuery(column, columns);
    }
}
