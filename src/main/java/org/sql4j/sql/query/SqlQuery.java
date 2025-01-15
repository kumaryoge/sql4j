package org.sql4j.sql.query;

import lombok.NonNull;

import java.util.Arrays;
import java.util.Objects;

public class SqlQuery {

    public static SelectQuery select(@NonNull Column<?> column, @NonNull Column<?>... columns) {
        Arrays.stream(columns).forEach(Objects::requireNonNull);
        return new SelectQuery(column, columns);
    }
}
