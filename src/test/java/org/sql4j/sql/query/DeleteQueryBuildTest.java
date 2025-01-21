package org.sql4j.sql.query;

import org.junit.jupiter.api.Test;
import org.sql4j.sql.query.DeleteQuery.ExecutableDeleteQuery;

import java.sql.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DeleteQueryBuildTest {
    private static final Table TABLE_1 = Table.forName("TABLE_1");
    private static final Column<String> COL_1 = Column.forName("COL_1");
    private static final Column<Integer> COL_2 = Column.forName("COL_2");
    private static final Column<Double> COL_3 = Column.forName("COL_3");
    private static final Column<Date> COL_4 = Column.forName("COL_4");

    @Test
    void testDeleteQuery_allRecords() {
        String expectedSql = """
                DELETE
                FROM
                    TABLE_1
                """;

        ExecutableDeleteQuery query =
                SqlQuery.delete()
                        .from(TABLE_1);

        assertEquals(expectedSql, query.sql());
        assertEquals(0, query.params().size());
    }

    @Test
    void testDeleteQuery_selectedRecords_singleCondition() {
        String expectedSql = """
                DELETE
                FROM
                    TABLE_1
                WHERE
                    COL_1 = ?
                """;

        ExecutableDeleteQuery query =
                SqlQuery.delete()
                        .from(TABLE_1)
                        .where(COL_1.equalTo("test"));

        assertEquals(expectedSql, query.sql());
        assertEquals(1, query.params().size());
    }

    @Test
    void testDeleteQuery_selectedRecords_multipleConditions() {
        Date currentDate = new Date(System.currentTimeMillis());

        String expectedSql = """
                DELETE
                FROM
                    TABLE_1
                WHERE
                    COL_1 = ?
                    AND COL_2 = ?
                     OR COL_3 = ?
                     OR COL_4 = ?
                """;

        ExecutableDeleteQuery query =
                SqlQuery.delete()
                        .from(TABLE_1)
                        .where(COL_1.equalTo("test")
                                .and(COL_2.equalTo(1))
                                .or(COL_3.equalTo(2.0))
                                .or(COL_4.equalTo(currentDate)));

        assertEquals(expectedSql, query.sql());
        assertEquals(4, query.params().size());
    }

    @Test
    void testDeleteQuery_selectedRecords_multipleConditions2() {
        Date currentDate = new Date(System.currentTimeMillis());

        String expectedSql = """
                DELETE
                FROM
                    TABLE_1
                WHERE
                    COL_1 = ?
                    AND (COL_2 = ?
                     OR COL_3 = ?)
                     OR COL_4 = ?
                """;

        ExecutableDeleteQuery query =
                SqlQuery.delete()
                        .from(TABLE_1)
                        .where(COL_1.equalTo("test")
                                .and(COL_2.equalTo(1)
                                        .or(COL_3.equalTo(2.0)))
                                .or(COL_4.equalTo(currentDate)));

        assertEquals(expectedSql, query.sql());
        assertEquals(4, query.params().size());
    }
}
