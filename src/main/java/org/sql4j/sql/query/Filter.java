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

    Filter(@NonNull String condition, @NonNull List<Object> params) {
        Utils.requireNonNulls(params);
        this.condition = condition;
        this.params = params;
    }

    public Filter and(@NonNull Filter other) {
        return new Filter(condition + "\n    AND " + other.condition, Stream.concat(params.stream(), other.params.stream()).toList());
    }

    public Filter or(@NonNull Filter other) {
        return new Filter(condition + "\n     OR " + other.condition, Stream.concat(params.stream(), other.params.stream()).toList());
    }

    public Filter negate() {
        return new Filter("NOT " + condition, params.stream().toList());
    }
}
