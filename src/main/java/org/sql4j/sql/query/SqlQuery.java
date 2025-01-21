package org.sql4j.sql.query;

import lombok.NonNull;

public class SqlQuery {

    public static SelectQuery select(@NonNull Column<?> column, @NonNull Column<?>... columns) {
        Utils.requireNonNulls(columns);
        return new SelectQuery(column, columns);
    }

    public static DeleteQuery delete() {
        return new DeleteQuery();
    }

    public static InsertQuery insert() {
        return new InsertQuery();
    }

    public static UpdateQuery update(@NonNull Table table) {
        return new UpdateQuery(table);
    }
}
