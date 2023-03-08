package io.trino.tpch

import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver
import java.util.*

fun main() {
    val scaleFactor = 0.1
    val part = 1
    val numberOfParts = 1
    val batchSize = 100

    val start = System.currentTimeMillis();
    val driver = ArrowFlightJdbcDriver()
    val url = "jdbc:arrow-flight://127.0.0.1:50060"
    val props = Properties()
    props.setProperty("useEncryption", "false")
    props.setProperty("user", "admin")
    props.setProperty("password", "password")
    driver.connect(url, props).use { con ->
        con.createStatement().use { stmt ->
            val q1 = """select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem
where
        l_shipdate <= date '1998-09-02'
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;"""
            val rs = stmt.executeQuery(q1)
            while(rs.next()) {
                // print results
            }
        }
    }
}