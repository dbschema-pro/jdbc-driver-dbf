package com.wisecoders.jdbc.dbf

import java.sql.DriverManager
import java.sql.SQLException
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class TestAllDataFolders {

    @ParameterizedTest
    @ValueSource(
        strings = [
            "dbase3plus",
            "dbase4",
            "dbase5",
            "clipper5",
            "foxpro26",
        ]
    ) @Throws(SQLException::class) fun testDriver(
        dataDirectory: String,
    ) {
        JdbcDriver()
        val URL = "jdbc:dbschema:dbf:src/test/resources/$dataDirectory/cars"
        DriverManager.getConnection(URL)
    }

}
