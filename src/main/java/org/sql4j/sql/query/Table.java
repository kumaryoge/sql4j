package org.sql4j.sql.query;

import lombok.Getter;
import lombok.NonNull;

@Getter
public class Table {
    public static final Table TABLE_1 = new Table("TABLE_1");
    public static final Table TABLE_2 = new Table("TABLE_2");

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

    public Table as(String alias) {
        return new Table(name, alias);
    }
}
