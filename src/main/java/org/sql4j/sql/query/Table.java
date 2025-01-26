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

/**
 * Class representing a table used in sql queries
 */

@Getter
public class Table {
    @NonNull
    private final String name;
    private final String alias;

    private Table(@NonNull String name) {
        this(name, null);
    }

    private Table(@NonNull String name, String alias) {
        this.name = name;
        this.alias = alias;
    }

    /**
     * @param name a table name used in sql queries
     * @return a Table object with the given table {@code name}
     */
    public static Table forName(@NonNull String name) {
        return new Table(name);
    }

    /**
     * @param alias alias for a table name used in sql queries e.g. {@code SELECT * FROM TABLE_1 AS T_1} (here T_1 is an alias for TABLE_1)
     * @return a copy of the caller Table object with the given {@code alias}
     */
    public Table as(String alias) {
        return new Table(name, Utils.enquoteAliasWithSpaces(alias));
    }
}
