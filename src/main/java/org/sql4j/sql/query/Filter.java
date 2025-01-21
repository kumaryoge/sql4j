package org.sql4j.sql.query;

import lombok.Getter;
import lombok.NonNull;

import java.util.List;
import java.util.stream.Stream;

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

    public Filter and(@NonNull Filter other) {
        return new Filter(condition + "\n    AND " + conditionStr(other.condition, other.isComposite),
                Stream.concat(params.stream(), other.params.stream()).toList(),
                true);
    }

    public Filter or(@NonNull Filter other) {
        return new Filter(condition + "\n     OR " + conditionStr(other.condition, other.isComposite),
                Stream.concat(params.stream(), other.params.stream()).toList(),
                true);
    }

    public Filter negate() {
        return new Filter("NOT " + conditionStr(condition, isComposite), params.stream().toList());
    }

    private String conditionStr(String condition, boolean isComposite) {
        return isComposite ? ("(" + condition + ")") : condition;
    }
}
