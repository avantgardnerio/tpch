package io.trino.tpch

import io.trino.tpch.generators.CustomerGenerator
import io.trino.tpch.generators.ItemGenerator
import io.trino.tpch.generators.LineItemGenerator
import io.trino.tpch.generators.NationGenerator
import io.trino.tpch.generators.OrderGenerator
import io.trino.tpch.generators.PartGenerator
import io.trino.tpch.generators.PartSupplierGenerator
import io.trino.tpch.generators.RegionGenerator
import io.trino.tpch.generators.SupplierGenerator
import org.apache.arrow.driver.jdbc.ArrowFlightConnection
import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver

import java.sql.PreparedStatement
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
            // Schema
            assert(stmt.execute("create table region (r_regionkey int, r_name varchar, r_comment varchar, primary key (r_regionkey))"))
            assert(stmt.execute("create table nation (n_nationkey int, n_name varchar, n_regionkey int, n_comment varchar, primary key (n_nationkey))"))

            assert(stmt.execute("""create table part (
                |p_partkey int, 
                |p_name varchar, 
                |p_mfgr varchar, 
                |p_brand varchar, 
                |p_type varchar, 
                |p_size int, 
                |p_container varchar, 
                |p_retailprice float, 
                |p_comment varchar, 
                |primary key (p_partkey))""".trimMargin()))

            assert(stmt.execute("create table supplier (" +
                    "s_suppkey int, " +
                    "s_name varchar, " +
                    "s_address varchar, " +
                    "s_nationkey int, " +
                    "s_phone varchar, " +
                    "s_acctbal float, " +
                    "s_comment varchar, " +
                    "primary key (s_suppkey))")) // FK to nation

            assert(stmt.execute("""create table partsupp (
                |ps_partkey int, 
                |ps_suppkey int, 
                |ps_availqty int, 
                |ps_supplycost float, 
                |ps_comment varchar, 
                |primary key (ps_partkey, ps_suppkey))""".trimMargin())) // FK to part & supplier

            assert(stmt.execute("""create table customer (
                |c_custkey int, 
                |c_name varchar, 
                |c_address varchar, 
                |c_nationkey int, 
                |c_phone varchar, 
                |c_acctbal float, 
                |c_mktsegment varchar, 
                |c_comment varchar, 
                |primary key (c_custkey))""".trimMargin())) // FK to nation

            assert(stmt.execute("""create table orders (
                |o_orderkey int, 
                |o_custkey int, 
                |o_orderstatus varchar, 
                |o_totalprice float, 
                |o_orderdate timestamp, 
                |o_orderpriority varchar, 
                |o_clerk varchar, 
                |o_shippriority int, 
                |o_comment varchar, 
                |primary key (o_orderkey))""".trimMargin())) // FK to customer

            assert(stmt.execute("""create table lineitem (
                |l_orderkey int, 
                |l_partkey int, 
                |l_suppkey int, 
                |l_linenumber int, 
                |l_quantity float, 
                |l_extendedprice float, 
                |l_discount float, 
                |l_tax float, 
                |l_returnflag varchar, 
                |l_linestatus varchar, 
                |l_shipdate timestamp, 
                |l_commitdate timestamp, 
                |l_receiptdate timestamp, 
                |l_shipinstruct varchar, 
                |l_shipmode varchar, 
                |l_comment varchar, 
                |primary key (l_orderkey, l_linenumber))""".trimMargin()))
        }

        // Data
        insert(RegionGenerator(), batchSize, con)
        insert(NationGenerator(), batchSize, con)
        insert(PartGenerator(scaleFactor, part, numberOfParts), batchSize, con)
        insert(SupplierGenerator(scaleFactor, part, numberOfParts), batchSize, con)
        insert(PartSupplierGenerator(scaleFactor, part, numberOfParts), batchSize, con)
        insert(CustomerGenerator(scaleFactor, part, numberOfParts), batchSize, con)
        insert(OrderGenerator(scaleFactor, part, numberOfParts), batchSize, con)
        insert(LineItemGenerator(scaleFactor, part, numberOfParts), batchSize, con)

        val end = System.currentTimeMillis()
        println("Generated data in ${(end - start) / 1000} seconds")
    }
}

private fun <T: TpchEntity?>insert(
    nationGen: ItemGenerator<T>,
    batchSize: Int,
    con: ArrowFlightConnection
) {
    var ps: PreparedStatement? = null
    var lastCount = -1
    nationGen.chunked(batchSize).forEach { batch ->
        if (batch.size != lastCount) {
            val sql = nationGen.getInsertStmt(minOf(batch.size, batchSize))
            println(sql)
            ps = con.prepareStatement(sql)
            lastCount = batch.size
        }
        println("Inserting ${batch.size} rows...")
        batch.forEachIndexed { idx, el -> el!!.setParams(ps!!, idx) }
        ps!!.executeUpdate()
    }
}
