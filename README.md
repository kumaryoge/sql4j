# Sql4j
This is a simple java library to simplify building and executing sql queries in java.

# Usage

```java
List<String> results =
        SqlQuery.select(COL_1)
                .from(TABLE_1)
                .execute(connection, rs -> rs.getString(COL_1.getName()));
```

```java
List<Table1Row> results =
                SqlQuery.select(COL_1, COL_2)
                        .from(TABLE_1)
                        .where(COL_1.equalTo("test1"))
                        .execute(connection, rs -> Table1Row.builder()
                                .col1(rs.getString(COL_1.getName()))
                                .col2(rs.getInt(COL_2.getName()))
                                .build());
```

Checkout more examples in
* [SelectQueryBuildTest.java](src/test/java/org/sql4j/sql/query/SelectQueryBuildTest.java) for building sql queries, and
* [SelectQueryExecuteTest.java](src/test/java/org/sql4j/sql/query/SelectQueryExecuteTest.java) for executing sql queries
