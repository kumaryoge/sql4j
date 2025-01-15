package org.sql4j.sql.query;

import lombok.Getter;
import lombok.NonNull;

import java.util.Arrays;
import java.util.Objects;

@Getter
public class Filter {
    @NonNull
    private final String condition;
    @NonNull
    private final Object[] objects;

    Filter(@NonNull String condition, @NonNull Object... objects) {
        Arrays.stream(objects).forEach(Objects::requireNonNull);
        this.condition = condition;
        this.objects = objects;
    }

    public Filter and(@NonNull Filter other) {
        return new Filter(condition + "\n    AND " + other.condition, objects, other.objects);
    }

    public Filter or(@NonNull Filter other) {
        return new Filter(condition + "\n     OR " + other.condition, objects, other.objects);
    }

    public Filter negate() {
        return new Filter("NOT " + condition, objects);
    }
}
