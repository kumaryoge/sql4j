package org.sql4j.sql.query;

import lombok.Getter;
import lombok.NonNull;

/**
 * Class representing a table used in sql queries
 */

@Getter
public class Table {
    @NonNull
    private final String name;
    private final String alias;

    private Table(@NonNull String name) {
        this(name, null);
    }

    private Table(@NonNull String name, String alias) {
        this.name = name;
        this.alias = alias;
    }

    /**
     * @param name a table name used in sql queries
     * @return a Table object with the given table {@code name}
     */
    public static Table forName(@NonNull String name) {
        return new Table(name);
    }

    /**
     * @param alias alias for a table name used in sql queries e.g. {@code SELECT * FROM TABLE_1 AS T_1} (here T_1 is an alias for TABLE_1)
     * @return a copy of the caller Table object with the given {@code alias}
     */
    public Table as(String alias) {
        return new Table(name, alias);
    }
}
