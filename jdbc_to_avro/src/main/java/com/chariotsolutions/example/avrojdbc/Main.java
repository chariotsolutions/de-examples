package com.chariotsolutions.example.avrojdbc;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 *  Driver program for exporting database tables as Avro files. Makes a query, and
 *  invokes the {@link ResultSetHandler} to write the results as an Avro file.
 *  <p>
 *  As written, this is usable with Postgres databases, and uses the standard Postgres
 *  environment variables (PGHOST, PGPORT, PGUSER, PGPASSWORD, and PGDATABASE) to open
 *  a connection.
 *  <p>
 *  Invocation:
 *
 *      Main TABLE_NAME OUTPUT_FILE_PATH
 */
public class Main
{
    public static void main(String[] argv)
    throws Exception
    {
        // pgJDBC uses java.util.logging; we want to redirect through SLF4J, which requires some monkey-patching
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        if (argv.length != 2)
        {
            System.err.println("invocation: Main TABLE OUTPUT_FILE");
            System.exit(1);
        }

        String tableName = argv[0];
        File outputFile = new File(argv[1]);

        try (Connection cxt = openConnection();
             Statement stmt = cxt.createStatement();
             ResultSet rslt = stmt.executeQuery("select * from " + tableName))
        {
            ResultSetHandler.writeAvro("com.chariotsolutions.example", tableName, rslt, outputFile);
        }
    }


    /**
     *  Opens a connection to the database using the standard Postgres environment variables.
     */
    private static Connection openConnection()
    throws SQLException
    {
        String host     = System.getenv("PGHOST");
        String port     = System.getenv("PGPORT");
        String username = System.getenv("PGUSER");
        String password = System.getenv("PGPASSWORD");
        String database = System.getenv("PGDATABASE");

        String url = "jdbc:postgresql://" + host + ":" + port + "/" + database;
        return DriverManager.getConnection(url, username, password);
    }
}