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

/**
 * Class used to build and execute {@code SQL} queries
 */

public class SqlQuery {

    /**
     * @param column a {@link Column} to be selected
     * @param columns other {@link Column}s to be selected
     * @return a {@link SelectQuery} with the given {@code column} and {@code columns} e.g. {@code SELECT column, <columns separated by comma> ...}
     */
    public static SelectQuery select(@NonNull Column<?> column, @NonNull Column<?>... columns) {
        Utils.requireNonNulls(columns);
        return new SelectQuery(column, columns);
    }

    /**
     * @return a {@link DeleteQuery}
     */
    public static DeleteQuery delete() {
        return new DeleteQuery();
    }

    /**
     * @return an {@link InsertQuery}
     */
    public static InsertQuery insert() {
        return new InsertQuery();
    }

    /**
     * @param table a table to be updated
     * @return an {@link UpdateQuery} with the given {@code table} e.g. {@code UPDATE table ...}
     */
    public static UpdateQuery update(@NonNull Table table) {
        return new UpdateQuery(table);
    }
}
