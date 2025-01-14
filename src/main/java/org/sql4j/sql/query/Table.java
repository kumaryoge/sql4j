package org.sql4j.sql.query;

import lombok.Getter;

@Getter
public class Table {
    public static final Table TABLE_1 = new Table("TABLE_1");
    public static final Table TABLE_2 = new Table("TABLE_2");

    private final String name;
    private final String alias;

    private Table(String name) {
        this(name, null);
    }

    private Table(String name, String alias) {
        this.name = name;
        this.alias = alias;
    }

    public Table as(String alias) {
        return new Table(name, alias);
    }
}
