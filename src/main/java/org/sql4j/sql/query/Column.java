package org.sql4j.sql.query;

import lombok.Getter;
import lombok.NonNull;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

@Getter
public class Column<T> {
    public static final Column<String> ALL = new Column<>("*");
    public static final Column<String> COL_1 = new Column<>("COL_1");
    public static final Column<Integer> COL_2 = new Column<>("COL_2");
    public static final Column<Double> COL_3 = new Column<>("COL_3");
    public static final Column<Date> COL_4 = new Column<>("COL_4");
    public static final Column<Time> COL_5 = new Column<>("COL_5");
    public static final Column<Timestamp> COL_6 = new Column<>("COL_6");

    @NonNull
    private final String name;
    private final String alias;
    private final Order order;

    private Column(@NonNull String name) {
        this(name, null);
    }

    private Column(@NonNull String name, String alias) {
        this(name, alias, null);
    }

    private Column(@NonNull String name, String alias, Order order) {
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
    public Column<T> of(@NonNull Table table) {
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

    public Filter equalTo(@NonNull T value) {
        return new Filter(name + " = ?", List.of(value));
    }

    public Filter notEqualTo(@NonNull T value) {
        return new Filter(name + " != ?", List.of(value));
    }

    public Filter greaterThan(@NonNull T value) {
        return new Filter(name + " > ?", List.of(value));
    }

    public Filter greaterThanOrEqualTo(@NonNull T value) {
        return new Filter(name + " >= ?", List.of(value));
    }

    public Filter lessThan(@NonNull T value) {
        return new Filter(name + " < ?", List.of(value));
    }

    public Filter lessThanOrEqualTo(@NonNull T value) {
        return new Filter(name + " <= ?", List.of(value));
    }

    public Filter between(@NonNull T value1, @NonNull T value2) {
        return new Filter(name + " BETWEEN ? AND ?", List.of(value1, value2));
    }

    public Filter notBetween(@NonNull T value1, @NonNull T value2) {
        return new Filter(name + " NOT BETWEEN ? AND ?", List.of(value1, value2));
    }

    public Filter like(@NonNull T value) {
        return new Filter(name + " LIKE ?", List.of(value));
    }

    public Filter notLike(@NonNull T value) {
        return new Filter(name + " NOT LIKE ?", List.of(value));
    }

    public Filter in(@NonNull T value, @NonNull T... values) {
        Arrays.stream(values).forEach(Objects::requireNonNull);
        return new Filter(name + " IN (?" + ", ?".repeat(values.length) + ")", (List<Object>) Stream.concat(Stream.of(value), Arrays.stream(values)).toList());
    }

    public Filter notIn(@NonNull T value, @NonNull T... values) {
        Arrays.stream(values).forEach(Objects::requireNonNull);
        return new Filter(name + " NOT IN (?" + ", ?".repeat(values.length) + ")", (List<Object>) Stream.concat(Stream.of(value), Arrays.stream(values)).toList());
    }

    public Filter isNull() {
        return new Filter(name + " IS NULL", List.of());
    }

    public Filter isNotNull() {
        return new Filter(name + " IS NOT NULL", List.of());
    }

    public enum Order {
        ASC, DESC
    }
}
