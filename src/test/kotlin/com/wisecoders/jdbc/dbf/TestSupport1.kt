package com.wisecoders.jdbc.dbf

import java.sql.DriverManager
import org.junit.jupiter.api.Test

class TestSupport1 {
    @Test
    @Throws(Exception::class)
    fun test() {
        JdbcDriver()
        val URL = "jdbc:dbschema:dbf:src/test/resources/support1"
        val con = DriverManager.getConnection(URL)
    }
}
