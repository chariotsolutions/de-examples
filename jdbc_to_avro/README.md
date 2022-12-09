# JDBC to Avro

This sub-project contains example code that reads the contents of a table using JDBC and
writes the data as an Avro file. It uses JDBC to attach to the database, and builds the
Avro schema from the JDBC `ResultSetMetaData` object.

> Note: this is prototype-quality code, intended to show how to build an Avro schema at
  runtime. It handles a limited set of database types, and has minimal error-handling.


## Building and Running

This project uses Maven to build an "uber-jar", which contains all necessary dependencies.
To build, run the following from the sub-project root directory (_not_ the repository root):

```
mvn clean package
```

To run, you must first set the following environment variables (there are no defaults):

  * `PGHOST`: hostname of the Postgres server.
  * `PGPORT`: port where Postgres is listening on that server.
  * `PGUSER`: a valid Postgres user.
  * `PGPASSWORD`: password for that user.
  * `PGDATABASE`: the default database to use when connecting.

Once you've done that:

```
java -jar target/jdbc_avro*.jar TABLE_NAME OUTPUT_FILE
```

* `TABLE_NAME` is the name of a table in the database you're connecting to.
* `OUTPUT_FILE` is the name of the file to write it to.

The program will construct a schema based on the columns in the table. Each field
in the schema uses the corresponding column name (beware that the table must use
SQL-standard identifiers: embedded spaces or non-alphanumeric characters [are not
permitted by Avro](https://avro.apache.org/docs/1.11.1/specification/#names)). The
column's data type is used to pick an appropriate Avro type, but beware that not
all database-provided types are supported.

While running, the program logs the schema that it built, and the number of rows
it processed.


## Program Structure

The program is based around two classes (and their subclasses):

* [ResultSetHandler](src/main/java/com/chariotsolutions/example/avrojdbc/ResultSetHandler.java)
  is responsible for creating `FieldHandler` instances from the result-set metadata, using those
  instances to create a schema for the overall table, and then for iterating the result-set and
  writing each row to the file.

* [FieldHandler](src/main/java/com/chariotsolutions/example/avrojdbc/field_handlers/FieldHandler.java)
  is the primary abstraction: there's one subclass for each database type that
  the program supports, and each column in the result-set is turned into an instance
  of the appropriate subclass. A `FieldHandler` instance has two purposes: generate a
  schema entry for the field, and translate the value returned by `ResultSet.getObject()`
  into its Avro representation.


## Example

Use Docker to start a Postgres database:

```
docker run -d --rm --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:12
```

Then set the environment variables that let the example program connect to this database:

```
export PGHOST=localhost
export PGPORT=5432
export PGUSER=postgres
export PGPASSWORD=postgres
export PGDATABASE=postgres
```

If you have the `psql` client installed, you can use it to verify these variables: run it
without any command-line arguments, and you should be able to connect.

If you don't have `psql` installed, you can run it from within the Docker container:

```
docker exec -it postgres psql --user postgres
```

This table exercises all of the Avro transformations:

```
create table test
(
  sval      text                        not null,
  ival      int                         not null,
  lval      bigint                      not null,
  fval      real,
  dval      double precision,
  nval      decimal(17,4),
  dtval     date,                        
  tsval     timestamp,
  tsvaltz   timestamp with time zone
);
```

Add a sample row:

```
insert into test (sval, ival, lval, fval, dval, nval, dtval, tsval, tsvaltz)
values ('abc', 123, 4567890123456789012, 1.1, 2.1, 3.1, current_date, current_timestamp, current_timestamp);
```

Then, you should be able to run the extractor, and see output like the following:

```
java -jar target/jdbc_avro-*.jar test test.avro
```
```
2022-12-09 13:22:07,056 DEBUG [main] c.c.e.a.ResultSetHandler - creating schema from 9 field handlers
2022-12-09 13:22:07,058 DEBUG [main] c.c.e.a.ResultSetHandler - schema: {"type":"record","name":"test","namespace":"com.chariotsolutions.example","doc":"","fields":[{"name":"sval","type":"string"},{"name":"ival","type":"int"},{"name":"lval","type":"long"},{"name":"fval","type":["float","null"]},{"name":"dval","type":["double","null"]},{"name":"nval","type":[{"type":"bytes","logicalType":"decimal","precision":17,"scale":4},"null"]},{"name":"dtval","type":[{"type":"int","logicalType":"date"},"null"]},{"name":"tsval","type":[{"type":"long","logicalType":"timestamp-millis"},"null"]},{"name":"tsvaltz","type":[{"type":"long","logicalType":"timestamp-millis"},"null"]}]}
2022-12-09 13:22:07,083 INFO  [main] c.c.e.a.ResultSetHandler - wrote 1 rows
```

Lastly, you can use the [avro-tools](https://search.maven.org/artifact/org.apache.avro/avro-tools/1.11.1/jar)
program to inspect the contents of the file:

```
java -jar $HOME/.m2/repository/org/apache/avro/avro-tools/1.11.1/avro-tools-1.11.1.jar tojson test.avro
```
```
{"sval":"abc","ival":123,"lval":4567890123456789012,"fval":{"float":1.1},"dval":{"double":2.1},"nval":{"bytes":"y\u0018"},"dtval":{"int":19335},"tsval":{"long":1670633100340},"tsvaltz":{"long":1670615100340}}
```

There are a few things that I want to call out from this example.

First, note that the fields with logical types are reported as their base types (eg, `tsval` as
`long`, `nval` as `bytes`).  You will need to load into some other tool (such as Amazon Athena)
to see the logical values.

Second, if you look at `tsval` and `tsvaltz` you'll see that the values are different, even though
they were both populated from `current_timestamp` (and the values are the same when retrieved with
`psql`). This happens because the Postgres JDBC driver interprets timestamps without timezones as
a _local_ timestamp. In other words: I inserted that value at 14:45:00 Eastern time, which was
19:45:00 UTC, and Postgres stored the value 19:45:00 in both fields; however, when I retrieved it,
the JDBC driver treated it as 19:45:00 _Eastern_ time. Not at all intuitive, and a reminder to
[Always use TIMESTAMP WITH TIME ZONE](https://justatheory.com/2012/04/postgres-use-timestamptz/).
