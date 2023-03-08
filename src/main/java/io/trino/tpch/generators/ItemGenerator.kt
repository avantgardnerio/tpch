package io.trino.tpch.generators

import io.trino.tpch.TpchEntity

interface ItemGenerator<T: TpchEntity?> : Iterable<T> {
    fun getInsertStmt(rowCount: Int): String;
}