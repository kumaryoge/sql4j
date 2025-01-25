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
