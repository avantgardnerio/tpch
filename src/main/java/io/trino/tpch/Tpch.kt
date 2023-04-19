package io.trino.tpch

import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver
import java.util.*

fun main() {
    val driver = ArrowFlightJdbcDriver()
    val url = "jdbc:arrow-flight://127.0.0.1:50060"
    val props = Properties()
    props.setProperty("useEncryption", "false")
    props.setProperty("user", "admin")
    props.setProperty("password", "password")
    driver.connect(url, props).use { con ->
        con.createStatement().use { stmt ->
            for (i in 1..22) {
                try {
                    var sql = GenerateUtils::class.java.getResource("queries/q$i.sql")!!.readText()
                    val stmts = sql.split(";").map { it.trim() }.filter { it.isNotBlank() }
                    if(stmts.size > 1) {
                        con.prepareStatement(stmts[0]).use { stmt ->
                            stmt.executeUpdate()
                        }
                        sql = stmts[1]
                    }
                    val start = System.currentTimeMillis();
                    val rs = stmt.executeQuery(sql)
                    var cnt = 0;
                    while (rs.next()) {
                        cnt++
                    }
                    val end = System.currentTimeMillis();
                    println("Query $i returned $cnt rows in ${end - start}ms")
                } catch (ex: Exception) {
                    println("Query $i failed with error: $ex")
                }
            }
        }
    }
}