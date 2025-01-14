package org.sql4j.sql.query;

import lombok.Getter;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Optional;

@Getter
public class Column<T> {
    public static final Column<String> ALL = new Column<>("*");
    public static final Column<String> COL_1 = new Column<>("COL_1");
    public static final Column<Integer> COL_2 = new Column<>("COL_2");
    public static final Column<Double> COL_3 = new Column<>("COL_3");
    public static final Column<Date> COL_4 = new Column<>("COL_4");
    public static final Column<Time> COL_5 = new Column<>("COL_5");
    public static final Column<Timestamp> COL_6 = new Column<>("COL_6");

    private final String name;
    private final String alias;
    private final Order order;

    private Column(String name) {
        this(name, null);
    }

    private Column(String name, String alias) {
        this(name, alias, null);
    }

    private Column(String name, String alias, Order order) {
        this.name = name;
        this.alias = alias;
        this.order = order;
    }

    public Column<T> as(String alias) {
        return new Column<>(name, alias, order);
    }

    public Column<T> asc() {
        return new Column<>(name, alias, Order.ASC);
    }

    public Column<T> desc() {
        return new Column<>(name, alias, Order.DESC);
    }

    /**
     * This allows using table alias (or name if alias doesn't exist) as a qualifier for column name in sql query
     * e.g. for table alias 'A' and Column name 'B', the new column name used in sql query would be 'A.B'
     */
    public Column<T> of(Table table) {
        return new Column<>(Optional.ofNullable(table.getAlias()).orElse(table.getName()) + "." + name, alias, order);
    }

    public Column<T> count() {
        return new Column<>("COUNT(" + name + ")", alias, order);
    }

    public Column<T> distinct() {
        return new Column<>("DISTINCT " + name, alias, order);
    }

    public Column<T> countDistinct() {
        return new Column<>("COUNT(DISTINCT " + name + ")", alias, order);
    }

    public Filter equalTo(T value) {
        return new Filter(name + " = " + quote(value));
    }

    public Filter notEqualTo(T value) {
        return new Filter(name + " != " + quote(value));
    }

    public Filter greaterThan(T value) {
        return new Filter(name + " > " + quote(value));
    }

    public Filter greaterThanOrEqualTo(T value) {
        return new Filter(name + " >= " + quote(value));
    }

    public Filter lessThan(T value) {
        return new Filter(name + " < " + quote(value));
    }

    public Filter lessThanOrEqualTo(T value) {
        return new Filter(name + " <= " + quote(value));
    }

    public Filter between(T value1, T value2) {
        return new Filter(name + " BETWEEN " + quote(value1) + " AND " + quote(value2));
    }

    public Filter notBetween(T value1, T value2) {
        return new Filter(name + " NOT BETWEEN " + quote(value1) + " AND " + quote(value2));
    }

    public Filter like(T value) {
        return new Filter(name + " LIKE " + quote(value));
    }

    public Filter notLike(T value) {
        return new Filter(name + " NOT LIKE " + quote(value));
    }

    public Filter in(T value, T... values) {
        return new Filter(name + " IN " + list(value, values));
    }

    public Filter notIn(T value, T... values) {
        return new Filter(name + " NOT IN " + list(value, values));
    }

    public Filter isNull() {
        return new Filter(name + " IS NULL");
    }

    public Filter isNotNull() {
        return new Filter(name + " IS NOT NULL");
    }

    private String list(T value, T[] values) {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        sb.append(quote(value));
        for (T val : values) {
            sb.append(", ");
            sb.append(quote(val));
        }
        sb.append(")");
        return sb.toString();
    }

    private String quote(T value) {
        return value instanceof Number ? value.toString() : "'" + value + "'";
    }

    public enum Order {
        ASC, DESC
    }
}
