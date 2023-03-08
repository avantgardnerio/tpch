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

class Order(
    override val rowNumber: Long,
    @JvmField val orderKey: Long,
    @JvmField val customerKey: Long,
    @JvmField val orderStatus: Char,
    val totalPriceInCents: Long,
    @JvmField val orderDate: Int,
    orderPriority: String,
    clerk: String,
    @JvmField val shipPriority: Int,
    comment: String
) : TpchEntity {
    @JvmField
    val orderPriority: String

    @JvmField
    val clerk: String

    @JvmField
    val comment: String

    init {
        this.orderPriority = Objects.requireNonNull(orderPriority, "orderPriority is null")
        this.clerk = Objects.requireNonNull(clerk, "clerk is null")
        this.comment = Objects.requireNonNull(comment, "comment is null")
    }

    fun getTotalPrice(): Double {
        return totalPriceInCents / 100.0
    }

    override fun toLine(): String? {
        return String.format(
            Locale.ENGLISH,
            "%d|%d|%s|%s|%s|%s|%s|%d|%s|",
            orderKey,
            customerKey,
            orderStatus,
            GenerateUtils.formatMoney(totalPriceInCents),
            GenerateUtils.formatDate(orderDate),
            orderPriority,
            clerk,
            shipPriority,
            comment
        )
    }

    override fun setParams(ps: PreparedStatement, rowIdx: Int) {
        val base = rowIdx * 9
        ps.setInt(base + 1, orderKey.toInt())
        ps.setInt(base + 2, customerKey.toInt())
        ps.setString(base + 3, orderStatus.toString())
        ps.setFloat(base + 4, totalPriceInCents.toFloat() / 100f)
        ps.setTimestamp(base + 5, Timestamp(orderDate.toLong()))
        ps.setString(base + 6, orderPriority)
        ps.setString(base + 7, clerk)
        ps.setInt(base + 8, shipPriority)
        ps.setString(base + 9, comment)
    }

}