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
