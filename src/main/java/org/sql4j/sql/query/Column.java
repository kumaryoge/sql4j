package org.sql4j.sql.query;

import lombok.Getter;
import lombok.NonNull;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

@Getter
public class Column<T> {
    public static final Column<String> ALL = Column.forName("*");

    @NonNull
    private final String name;
    /**
     * The SQL type as defined in {@link java.sql.Types}
     */
    private final int sqlType;
    private final String alias;
    private final Order order;

    private Column(@NonNull String name, int sqlType) {
        this(name, sqlType, null);
    }

    private Column(@NonNull String name, int sqlType, String alias) {
        this(name, sqlType, alias, null);
    }

    private Column(@NonNull String name, int sqlType, String alias, Order order) {
        this.name = name;
        this.sqlType = sqlType;
        this.alias = alias;
        this.order = order;
    }

    /**
     * Creates a column object for the given name.
     * <p>
     * This method can be safely used to create column object for SELECT and DELETE sql queries. For INSERT and UPDATE
     * sql queries, if you intend to set null value for a column, check another method {@link #forNameAndType}.
     *
     * @param name Column name in the database table
     * @return A column object for the given name
     * @param <T> Java class type for this column
     */
    public static <T> Column<T> forName(@NonNull String name) {
        return new Column<>(name, Types.NULL);
    }

    /**
     * Creates a column object for the given name and sqlType (as defined in {@link java.sql.Types}).
     * <p>
     * This method is recommended to use to create column object for INSERT and UPDATE sql queries if you intend to set
     * null value for the column, as it's safer to provide the column's sql type to the database backend when setting
     * null values because not all databases support passing non-typed null values to the database backend. Note that
     * null values in java are non-typed i.e. we can't tell the type of null object in java, the condition
     * {@code null instanceof <any java class or interface>} always returns false.
     *
     * @param name Column name in the database table
     * @param sqlType Column's SQL type as defined in {@link java.sql.Types}
     * @return A column object for the given name and sqlType
     * @param <T> Java class type for this column
     */
    public static <T> Column<T> forNameAndType(@NonNull String name, int sqlType) {
        return new Column<>(name, sqlType);
    }

    public Column<T> as(String alias) {
        return new Column<>(name, sqlType, alias, order);
    }

    public Column<T> asc() {
        return new Column<>(name, sqlType, alias, Order.ASC);
    }

    public Column<T> desc() {
        return new Column<>(name, sqlType, alias, Order.DESC);
    }

    /**
     * This allows using table alias (or name if alias doesn't exist) as a qualifier for column name in sql query
     * e.g. for table alias 'A' and Column name 'B', the new column name used in sql query would be 'A.B'
     */
    public Column<T> of(@NonNull Table table) {
        return new Column<>(Optional.ofNullable(table.getAlias()).orElse(table.getName()) + "." + name, sqlType, alias, order);
    }

    public Column<T> count() {
        return new Column<>("COUNT(" + name + ")", sqlType, alias, order);
    }

    public Column<T> distinct() {
        return new Column<>("DISTINCT " + name, sqlType, alias, order);
    }

    public Column<T> countDistinct() {
        return new Column<>("COUNT(DISTINCT " + name + ")", sqlType, alias, order);
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
        Utils.requireNonNulls(values);
        return new Filter(name + " IN (?" + ", ?".repeat(values.length) + ")", (List<Object>) Stream.concat(Stream.of(value), Arrays.stream(values)).toList());
    }

    public Filter notIn(@NonNull T value, @NonNull T... values) {
        Utils.requireNonNulls(values);
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
