package org.sql4j.sql.query;

import lombok.Getter;
import lombok.NonNull;

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

    public static Table forName(@NonNull String name) {
        return new Table(name);
    }

    public Table as(String alias) {
        return new Table(name, alias);
    }
}
