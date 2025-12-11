package com.wisecoders.jdbc.dbf

import java.sql.DriverManager
import java.sql.SQLException
import org.junit.jupiter.api.Test

class TestCars {
    @Test
    @Throws(SQLException::class)
    fun test() {
        JdbcDriver()
        run {
            val con = DriverManager.getConnection(URL)
            val st = con.createStatement()
            //st.execute("reload cars");
            if (st.execute("select * from cars")) {
                val rs = st.resultSet
                while (rs.next()) {
                    for (i in 0..<rs.metaData.columnCount) {
                        print(rs.getString(i + 1) + ",")
                    }
                    println()
                }
            }
            st.execute("save dbf to out/testExport")
        }

        run {
            val con = DriverManager.getConnection(URL)
            val st = con.createStatement()
            //st.execute("reload cars");
            if (st.execute("select * from cars")) {
                val rs = st.resultSet
                while (rs.next()) {
                    for (i in 0..<rs.metaData.columnCount) {
                        print(rs.getString(i + 1) + ",")
                    }
                    println()
                }
            }
            st.execute("save dbf to out/testExport")
        }
    }

    companion object {
        private const val URL = "jdbc:dbschema:dbf:src/test/resources/dbase5/cars?version=dbase_5"
    }
}
