[![Maven Central Version](https://img.shields.io/maven-central/v/io.github.kumaryoge/sql4j)](https://central.sonatype.com/artifact/io.github.kumaryoge/sql4j)

# Sql4j
This is a lightweight java library to simplify building and executing sql queries in java, without actually hard-coding any sql query in java code.

This library is published to [Maven Central Repository](https://central.sonatype.com/artifact/io.github.kumaryoge/sql4j) where you can find Snippets for adding a dependency on this library in Maven, Gradle and other types of projects.

# Usage
Suppose, in a database, we have a table TABLE_1 with two columns: COL_1 of type String (VARCHAR) and COL_2 of type Integer (INT).

```java
static final Table TABLE_1 = Table.forName("TABLE_1");
static final Column<String> COL_1 = Column.forName("COL_1");
static final Column<Integer> COL_2 = Column.forName("COL_2");
```

We can run queries like the following:

```java
List<String> results =
        SqlQuery.select(COL_1)
                .from(TABLE_1)
                .execute(connection, resultSet -> resultSet.getString(COL_1.getName()));
```

```java
List<Row> results =
        SqlQuery.select(COL_1, COL_2)
                .from(TABLE_1)
                .where(COL_1.equalTo("test1")
                        .and(COL_2.lessThan(2)))
                .execute(connection, resultSet -> new Row(
                        resultSet.getString(COL_1.getName()),
                        resultSet.getInt(COL_2.getName())));
// Row is defined as:
record Row(String col1, int col2) {}
```

```java
int numberOfDeletedRows =
        SqlQuery.delete()
                .from(TABLE_1)
                .where(COL_1.equalTo("test1"))
                .execute(connection);
```

```java
int numberOfInsertedRows =
        SqlQuery.insert()
                .into(TABLE_1)
                .values(COL_1.value("test1"), COL_2.value(1))
                .execute(connection);
```

```java
int numberOfUpdatedRows =
        SqlQuery.update(TABLE_1)
                .set(COL_2.value(2))
                .where(COL_1.equalTo("test1"))
                .execute(connection);
```

Where `connection` is a `java.sql.Connection` object that is created via `java.sql.DriverManager.getConnection(<database url>, <database user>, <user's password>)`.

Checkout more examples in
* [SelectQueryExecuteTest.java](src/test/java/org/sql4j/sql/query/integ/SelectQueryExecuteTest.java)
* [DeleteQueryExecuteTest.java](src/test/java/org/sql4j/sql/query/integ/DeleteQueryExecuteTest.java)
* [InsertQueryExecuteTest.java](src/test/java/org/sql4j/sql/query/integ/InsertQueryExecuteTest.java)
* [UpdateQueryExecuteTest.java](src/test/java/org/sql4j/sql/query/integ/UpdateQueryExecuteTest.java)
