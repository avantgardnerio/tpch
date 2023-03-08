/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tpch.models

import io.trino.tpch.GenerateUtils
import io.trino.tpch.TpchEntity
import java.sql.PreparedStatement
import java.sql.Timestamp
import java.util.*

class PartSupplier(
    override val rowNumber: Long,
    @JvmField val partKey: Long,
    @JvmField val supplierKey: Long,
    @JvmField val availableQuantity: Int,
    val supplyCostInCents: Long,
    comment: String
) : TpchEntity {
    @JvmField
    val comment: String

    init {
        this.comment = Objects.requireNonNull(comment, "comment is null")
    }

    fun getSupplyCost(): Double {
        return supplyCostInCents / 100.0
    }

    override fun toLine(): String? {
        return String.format(
            Locale.ENGLISH,
            "%d|%d|%d|%s|%s|",
            partKey,
            supplierKey,
            availableQuantity,
            GenerateUtils.formatMoney(supplyCostInCents),
            comment
        )
    }

    override fun setParams(ps: PreparedStatement, rowIdx: Int) {
        val base = rowIdx * 5
        ps.setInt(base + 1, partKey.toInt())
        ps.setInt(base + 2, supplierKey.toInt())
        ps.setInt(base + 3, availableQuantity.toInt())
        ps.setFloat(base + 4, supplyCostInCents.toFloat() / 100f)
        ps.setString(base + 5, comment)
    }

}