package org.sql4j.sql.query;

import lombok.Getter;

@Getter
public class Filter {
    private final String condition;

    Filter(String condition) {
        this.condition = condition;
    }

    public Filter and(Filter other) {
        return new Filter(condition + "\n    AND " + other.condition);
    }

    public Filter or(Filter other) {
        return new Filter(condition + "\n     OR " + other.condition);
    }

    public Filter negate() {
        return new Filter("NOT " + condition);
    }
}
