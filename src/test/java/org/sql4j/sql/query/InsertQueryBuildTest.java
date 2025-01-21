package org.sql4j.sql.query;

import org.junit.jupiter.api.Test;
import org.sql4j.sql.query.InsertQuery.ExecutableInsertQuery;

import java.sql.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class InsertQueryBuildTest {
    private static final Table TABLE_1 = Table.forName("TABLE_1");
    private static final Column<String> COL_1 = Column.forName("COL_1");
    private static final Column<Integer> COL_2 = Column.forName("COL_2");
    private static final Column<Double> COL_3 = Column.forName("COL_3");
    private static final Column<Date> COL_4 = Column.forName("COL_4");

    @Test
    void testInsertQuery_singleColumn() {
        String expectedSql = """
                INSERT
                INTO
                    TABLE_1
                    (COL_1)
                VALUES
                    (?)
                """;

        ExecutableInsertQuery query =
                SqlQuery.insert()
                        .into(TABLE_1)
                        .values(COL_1.value("test"));

        assertEquals(expectedSql, query.sql());
        assertEquals(1, query.params().size());
    }

    @Test
    void testInsertQuery_multipleColumns() {
        Date currentDate = new Date(System.currentTimeMillis());

        String expectedSql = """
                INSERT
                INTO
                    TABLE_1
                    (COL_1, COL_2, COL_3, COL_4)
                VALUES
                    (?, ?, ?, ?)
                """;

        ExecutableInsertQuery query =
                SqlQuery.insert()
                        .into(TABLE_1)
                        .values(COL_1.value("test"),
                                COL_2.value(1),
                                COL_3.value(1.0),
                                COL_4.value(currentDate));

        assertEquals(expectedSql, query.sql());
        assertEquals(4, query.params().size());
    }
}
