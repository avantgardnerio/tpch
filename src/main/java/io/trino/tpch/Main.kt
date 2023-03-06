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
        }
    }
}

/*
echo -e "c_custkey|c_name|c_address|c_nationkey|c_phone|c_acctbal|c_mktsegment|c_comment|junk\n$(cat customer.tbl)" > customer.csv
echo -e "l_orderkey|l_partkey|l_suppkey|l_linenumber|l_quantity|l_extendedprice|l_discount|l_tax|l_returnflag|l_linestatus|l_shipdate|l_commitdate|l_receiptdate|l_shipinstruct|l_shipmode|l_comment|junk\n$(cat lineitem.tbl)" > lineitem.csv
echo -e "o_orderkey|o_custkey|o_orderstatus|o_totalprice|o_orderdate|o_orderpriority|o_clerk|o_shippriority|o_comment|junk\n$(cat orders.tbl)" > orders.csv
echo -e "p_partkey|p_name|p_mfgr|p_brand|p_type|p_size|p_container|p_retailprice|p_comment|junk\n$(cat part.tbl)" > part.csv
echo -e "ps_partkey|ps_suppkey|ps_availqty|ps_supplycost|ps_comment|junk\n$(cat partsupp.tbl)" > partsupp.csv
echo -e "s_suppkey|s_name|s_address|s_nationkey|s_phone|s_acctbal|s_comment|junk\n$(cat supplier.tbl)" > supplier.csv

 */