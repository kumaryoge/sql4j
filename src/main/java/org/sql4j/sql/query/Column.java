package org.sql4j.sql.query;

/*-
 * ==============================LICENSE_START==============================
 * io.github.kumaryoge:sql4j
 * --
 * Copyright (C) 2025 io.github.kumaryoge
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ===============================LICENSE_END===============================
 */

import lombok.Getter;
import lombok.NonNull;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Class representing a column used in sql queries
 * @param <T> Java class type for this column e.g. String, Integer etc.
 */

@Getter
public class Column<T> {
    /**
     * A column object used in sql {@code SELECT} queries to fetch all {@code *} columns from a table
     */
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
     * @param name Column name in a database table
     * @return A column object for the given {@code name}
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
     * @param name Column name in a database table
     * @param sqlType Column's SQL type as defined in {@link java.sql.Types}
     * @return A column object for the given {@code name} and {@code sqlType}
     * @param <T> Java class type for this column
     */
    public static <T> Column<T> forNameAndType(@NonNull String name, int sqlType) {
        return new Column<>(name, sqlType);
    }

    /**
     * @param alias alias for a column name used in sql queries e.g. {@code SELECT COL_1 AS C_1 FROM TABLE_1} (here C_1 is an alias for COL_1)
     * @return a copy of the caller Column object with the given {@code alias}
     */
    public Column<T> as(String alias) {
        return new Column<>(name, sqlType, Utils.enquoteAliasWithSpaces(alias), order);
    }

    /**
     * @return a copy of the caller Column object with the {@code ASC} order used in {@code ORDER BY} clause
     */
    public Column<T> asc() {
        return new Column<>(name, sqlType, alias, Order.ASC);
    }

    /**
     * @return a copy of the caller Column object with the {@code DESC} order used in {@code ORDER BY} clause
     */
    public Column<T> desc() {
        return new Column<>(name, sqlType, alias, Order.DESC);
    }

    /**
     * This allows using table alias (or name if alias doesn't exist) as a qualifier for column name in sql query
     * e.g. for table alias {@code A} and Column name {@code B}, the new column name used in sql query would be {@code A.B}
     * @param table table used to qualify the caller column's name in sql query
     * @return a copy of the caller Column object with qualified name
     */
    public Column<T> of(@NonNull Table table) {
        return new Column<>(Optional.ofNullable(table.getAlias()).orElse(table.getName()) + "." + name, sqlType, alias, order);
    }

    /**
     * @return a copy of the caller Column object with a new name {@code MIN(<column name>)} used to return minimum value of this column
     */
    public Column<T> min() {
        return new Column<>("MIN(" + name + ")", sqlType, alias, order);
    }

    /**
     * @return a copy of the caller Column object with a new name {@code MAX(<column name>)} used to return maximum value of this column
     */
    public Column<T> max() {
        return new Column<>("MAX(" + name + ")", sqlType, alias, order);
    }

    /**
     * @return a copy of the caller Column object with a new name {@code SUM(<column name>)} used to return sum of all values of this column
     */
    public Column<T> sum() {
        return new Column<>("SUM(" + name + ")", sqlType, alias, order);
    }

    /**
     * @return a copy of the caller Column object with a new name {@code AVG(<column name>)} used to return average of all values of this column
     */
    public Column<T> avg() {
        return new Column<>("AVG(" + name + ")", sqlType, alias, order);
    }

    /**
     * @return a copy of the caller Column object with a new name {@code COUNT(<column name>)} used to return a count of all values of this column
     */
    public Column<T> count() {
        return new Column<>("COUNT(" + name + ")", sqlType, alias, order);
    }

    /**
     * @return a copy of the caller Column object with a new name {@code DISTINCT <column name>} used to return distinct values of this column
     */
    public Column<T> distinct() {
        return new Column<>("DISTINCT " + name, sqlType, alias, order);
    }

    /**
     * @return a copy of the caller Column object with a new name {@code COUNT(DISTINCT <column name>)} used to return a count of distinct values of this column
     */
    public Column<T> countDistinct() {
        return new Column<>("COUNT(DISTINCT " + name + ")", sqlType, alias, order);
    }

    /**
     * @param value a value of the caller Column, to be inserted or updated in a table
     * @return a {@link ColumnValue} object representing the caller Column and its given {@code value}
     */
    public ColumnValue value(T value) {
        return new ColumnValue(name, sqlType, value);
    }

    /**
     * @param value a value of the caller Column
     * @return a {@link Filter} object with the condition {@code column = value}
     */
    public Filter equalTo(@NonNull T value) {
        return new Filter(name + " = ?", List.of(value));
    }

    /**
     * @param value a value of the caller Column
     * @return a {@link Filter} object with the condition {@code column != value}
     */
    public Filter notEqualTo(@NonNull T value) {
        return new Filter(name + " != ?", List.of(value));
    }

    /**
     * @param value a value of the caller Column
     * @return a {@link Filter} object with the condition {@code column > value}
     */
    public Filter greaterThan(@NonNull T value) {
        return new Filter(name + " > ?", List.of(value));
    }

    /**
     * @param value a value of the caller Column
     * @return a {@link Filter} object with the condition {@code column >= value}
     */
    public Filter greaterThanOrEqualTo(@NonNull T value) {
        return new Filter(name + " >= ?", List.of(value));
    }

    /**
     * @param value a value of the caller Column
     * @return a {@link Filter} object with the condition {@code column < value}
     */
    public Filter lessThan(@NonNull T value) {
        return new Filter(name + " < ?", List.of(value));
    }

    /**
     * @param value a value of the caller Column
     * @return a {@link Filter} object with the condition {@code column <= value}
     */
    public Filter lessThanOrEqualTo(@NonNull T value) {
        return new Filter(name + " <= ?", List.of(value));
    }

    /**
     * @param value1 a value of the caller Column
     * @param value2 another value of the caller Column
     * @return a {@link Filter} object with the condition {@code column BETWEEN value1 AND value2}
     */
    public Filter between(@NonNull T value1, @NonNull T value2) {
        return new Filter(name + " BETWEEN ? AND ?", List.of(value1, value2));
    }

    /**
     * @param value1 a value of the caller Column
     * @param value2 another value of the caller Column
     * @return a {@link Filter} object with the condition {@code column NOT BETWEEN value1 AND value2}
     */
    public Filter notBetween(@NonNull T value1, @NonNull T value2) {
        return new Filter(name + " NOT BETWEEN ? AND ?", List.of(value1, value2));
    }

    /**
     * @param value a value of the caller Column
     * @return a {@link Filter} object with the condition {@code column LIKE value}
     */
    public Filter like(@NonNull T value) {
        return new Filter(name + " LIKE ?", List.of(value));
    }

    /**
     * @param value a value of the caller Column
     * @return a {@link Filter} object with the condition {@code column NOT LIKE value}
     */
    public Filter notLike(@NonNull T value) {
        return new Filter(name + " NOT LIKE ?", List.of(value));
    }

    /**
     * @param value a value of the caller Column
     * @param values other values of the caller Column
     * @return a {@link Filter} object with the condition {@code column IN (value, <values separated by comma>)}
     */
    public Filter in(@NonNull T value, @NonNull T... values) {
        Utils.requireNonNulls(values);
        return new Filter(name + " IN (?" + ", ?".repeat(values.length) + ")", (List<Object>) Stream.concat(Stream.of(value), Arrays.stream(values)).toList());
    }

    /**
     * @param value a value of the caller Column
     * @param values other values of the caller Column
     * @return a {@link Filter} object with the condition {@code column NOT IN (value, <values separated by comma>)}
     */
    public Filter notIn(@NonNull T value, @NonNull T... values) {
        Utils.requireNonNulls(values);
        return new Filter(name + " NOT IN (?" + ", ?".repeat(values.length) + ")", (List<Object>) Stream.concat(Stream.of(value), Arrays.stream(values)).toList());
    }

    /**
     * @return a {@link Filter} object with the condition {@code column IS NULL}
     */
    public Filter isNull() {
        return new Filter(name + " IS NULL", List.of());
    }

    /**
     * @return a {@link Filter} object with the condition {@code column IS NOT NULL}
     */
    public Filter isNotNull() {
        return new Filter(name + " IS NOT NULL", List.of());
    }

    private enum Order {
        ASC, DESC
    }
}
