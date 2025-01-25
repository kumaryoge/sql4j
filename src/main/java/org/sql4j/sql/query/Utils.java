package org.sql4j.sql.query;

import lombok.NonNull;

import java.util.Arrays;
import java.util.Objects;

class Utils {

    static void requireNonNulls(@NonNull Object[] objects) {
        requireNonNulls(Arrays.asList(objects));
    }

    static void requireNonNulls(@NonNull Iterable<Object> objects) {
        objects.forEach(obj -> Objects.requireNonNull(obj, "null value is provided where non-null is required"));
    }
}
