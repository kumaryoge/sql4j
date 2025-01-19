# Sql4j
This is a simple java library to simplify building and executing sql queries in java, without actually hard-coding any sql query in java code.

# Usage
Suppose, in a database, we have a table TABLE_1 with two columns: COL_1 of type String (VARCHAR) and COL_2 of type Integer (INT).

```java
private static final Table TABLE_1 = Table.forName("TABLE_1");
private static final Column<String> COL_1 = Column.forName("COL_1");
private static final Column<Integer> COL_2 = Column.forName("COL_2");
```

We can run queries like the following:

```java
List<String> results =
        SqlQuery.select(COL_1)
                .from(TABLE_1)
                .execute(connection, resultSet -> resultSet.getString(COL_1.getName()));
```

```java
List<Table1Row> results =
                SqlQuery.select(COL_1, COL_2)
                        .from(TABLE_1)
                        .where(COL_1.equalTo("test1")
                                .and(COL_2.lessThan(2)))
                        .execute(connection, resultSet -> Table1Row.builder()
                                .col1(resultSet.getString(COL_1.getName()))
                                .col2(resultSet.getInt(COL_2.getName()))
                                .build());

@lombok.Builder
private record Table1Row(String col1, int col2) {}
```

Where `connection` is a `java.sql.Connection` object that is created via `java.sql.DriverManager.getConnection(<database url>, <database user>, <user's password>)`.

Checkout more examples in
* [SelectQueryBuildTest.java](src/test/java/org/sql4j/sql/query/SelectQueryBuildTest.java)
* [SelectQueryExecuteTest.java](src/test/java/org/sql4j/sql/query/SelectQueryExecuteTest.java)
