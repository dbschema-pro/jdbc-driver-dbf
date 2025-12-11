package com.wisecoders.jdbc.dbf

import java.sql.DriverManager
import java.sql.SQLException
import org.junit.jupiter.api.Test

class TestLargeDataSet {
    @Test
    @Throws(SQLException::class)
    fun test() {
        JdbcDriver()
        run {
            val con = DriverManager.getConnection(URL)
            val st = con.createStatement()
            //st.execute("reload cars");
            if (st.execute("select * from lpp_histo_tot682")) {
                val rs = st.resultSet
                while (rs.next()) {
                    for (i in 0..<rs.metaData.columnCount) {
                        //System.out.print(rs.getString(i + 1) + ",");
                    }
                    //System.out.println();
                }
            }
            st.execute("save dbf to out/testExport")
        }
    }

    companion object {
        private const val URL = "jdbc:dbschema:dbf:src/test/resources/largeDataSet/?charset=Cp850"
    }
}
