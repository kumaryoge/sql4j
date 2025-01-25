package org.sql4j.sql.query;

import lombok.NonNull;

/**
 * Class used to build and execute {@code SQL} queries
 */

public class SqlQuery {

    /**
     * @param column a {@link Column} to be selected
     * @param columns other {@link Column}s to be selected
     * @return a {@link SelectQuery} with the given {@code column} and {@code columns} e.g. {@code SELECT column, <columns separated by comma> ...}
     */
    public static SelectQuery select(@NonNull Column<?> column, @NonNull Column<?>... columns) {
        Utils.requireNonNulls(columns);
        return new SelectQuery(column, columns);
    }

    /**
     * @return a {@link DeleteQuery}
     */
    public static DeleteQuery delete() {
        return new DeleteQuery();
    }

    /**
     * @return an {@link InsertQuery}
     */
    public static InsertQuery insert() {
        return new InsertQuery();
    }

    /**
     * @param table a table to be updated
     * @return an {@link UpdateQuery} with the given {@code table} e.g. {@code UPDATE table ...}
     */
    public static UpdateQuery update(@NonNull Table table) {
        return new UpdateQuery(table);
    }
}
