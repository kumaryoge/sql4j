package org.sql4j.sql.query;

import lombok.Getter;
import lombok.NonNull;

/**
 * Class representing a column and its value used in sql insert/update queries
 */

@Getter
public class ColumnValue {
    @NonNull
    private final String name;
    /**
     * The SQL type as defined in {@link java.sql.Types}
     */
    private final int sqlType;
    private final Object value;

    ColumnValue(@NonNull String name, int sqlType, Object value) {
        this.name = name;
        this.sqlType = sqlType;
        this.value = value;
    }
}
