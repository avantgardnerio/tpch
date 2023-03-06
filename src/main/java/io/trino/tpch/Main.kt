package io.trino.tpch

import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver
import java.io.FileWriter

import java.io.Writer
import java.util.*

fun main(args: Array<String>) {
    println(args.contentToString())
    val writer: Writer = FileWriter("customer.csv")
    val scaleFactor = 1.0
    val part = 1
    val numberOfParts = 1
    for (entity in CustomerGenerator(scaleFactor, part, numberOfParts)) {
        writer.write(entity.toLine())
        writer.write('\n'.code)
    }

    val driver = ArrowFlightJdbcDriver()
    val url = "jdbc:arrow-flight://127.0.0.1:50060"
    val props = Properties()
    props.setProperty("useEncryption", "false")
    props.setProperty("user", "admin")
    props.setProperty("password", "password")
    driver.connect(url, props).use { con ->
        con.createStatement().use { stmt ->
            assert(stmt.execute("create table region (r_regionkey int, r_name varchar, r_comment varchar, primary key (r_regionkey))"))
            assert(stmt.execute("create table nation (n_nationkey int, n_name varchar, n_regionkey int, n_comment varchar, primary key (n_nationkey))"))
            assert(stmt.execute("create table part (p_partkey int, p_name varchar, p_mfgr varchar, p_brand varchar, p_type varchar, p_size int, p_container varchar, p_retailprice float, p_comment varchar, primary key (p_partkey))"))

            assert(stmt.execute("create table supplier (s_suppkey int, s_name varchar, s_address varchar, s_nationkey int, s_phone varchar, s_acctbal float, s_comment varchar, primary key (s_suppkey))")) // FK to nation

            assert(stmt.execute("create table partsupp (ps_partkey int, ps_suppkey int, ps_availqty int, ps_supplycost float, ps_comment varchar, primary key (ps_partkey, ps_suppkey))")) // FK to part & supplier

            assert(stmt.execute("create table customer (c_custkey int, c_name varchar, c_address varchar, c_nationkey int, c_phone varchar, c_acctbal float, c_mktsegment varchar, c_comment varchar, primary key (c_custkey))")) // FK to nation

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
    }
}

/*
echo -e ",junk\n$(cat lineitem.tbl)" > lineitem.csv

 */