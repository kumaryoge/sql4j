package org.sql4j.sql.query;

import lombok.Getter;
import lombok.NonNull;

import java.util.List;
import java.util.stream.Stream;

/**
 * Class representing a condition used in {@code WHERE} clause to filter results in sql queries
 */

@Getter
public class Filter {
    @NonNull
    private final String condition;
    @NonNull
    private final List<Object> params;
    private final boolean isComposite;

    Filter(@NonNull String condition, @NonNull List<Object> params) {
        this(condition, params, false);
    }

    private Filter(@NonNull String condition, @NonNull List<Object> params, boolean isComposite) {
        Utils.requireNonNulls(params);
        this.condition = condition;
        this.params = params;
        this.isComposite = isComposite;
    }

    /**
     * @param other another filter
     * @return a new filter representing a logical {@code AND} of the caller filter and the given {@code other} filter
     */
    public Filter and(@NonNull Filter other) {
        return new Filter(condition + "\n    AND " + conditionStr(other.condition, other.isComposite),
                Stream.concat(params.stream(), other.params.stream()).toList(),
                true);
    }

    /**
     * @param other another filter
     * @return a new filter representing a logical {@code OR} of the caller filter and the given {@code other} filter
     */
    public Filter or(@NonNull Filter other) {
        return new Filter(condition + "\n     OR " + conditionStr(other.condition, other.isComposite),
                Stream.concat(params.stream(), other.params.stream()).toList(),
                true);
    }

    /**
     * @return a new filter representing a logical {@code NOT} of the caller filter
     */
    public Filter negate() {
        return new Filter("NOT " + conditionStr(condition, isComposite), params.stream().toList());
    }

    private String conditionStr(String condition, boolean isComposite) {
        return isComposite ? ("(" + condition + ")") : condition;
    }
}
