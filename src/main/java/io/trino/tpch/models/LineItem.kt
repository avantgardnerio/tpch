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

class LineItem(
    override val rowNumber: Long,
    @JvmField val orderKey: Long,
    @JvmField val partKey: Long,
    @JvmField val supplierKey: Long,
    @JvmField val lineNumber: Int,
    @JvmField val quantity: Long,
    val extendedPriceInCents: Long,
    val discountPercent: Long,
    val taxPercent: Long,
    returnFlag: String,
    status: String,
    @JvmField val shipDate: Int,
    @JvmField val commitDate: Int,
    @JvmField val receiptDate: Int,
    shipInstructions: String,
    shipMode: String,
    comment: String
) : TpchEntity {
    @JvmField
    val returnFlag: String

    @JvmField
    val status: String

    @JvmField
    val shipInstructions: String

    @JvmField
    val shipMode: String

    @JvmField
    val comment: String

    init {
        this.returnFlag = Objects.requireNonNull(returnFlag, "returnFlag is null")
        this.status = Objects.requireNonNull(status, "status is null")
        this.shipInstructions = Objects.requireNonNull(shipInstructions, "shipInstructions is null")
        this.shipMode = Objects.requireNonNull(shipMode, "shipMode is null")
        this.comment = Objects.requireNonNull(comment, "comment is null")
    }

    fun getExtendedPrice(): Double {
        return extendedPriceInCents / 100.0
    }

    fun getDiscount(): Double {
        return discountPercent / 100.0
    }

    fun getTax(): Double {
        return taxPercent / 100.0
    }

    override fun toLine(): String? {
        return String.format(
            Locale.ENGLISH,
            "%d|%d|%d|%d|%d|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|",
            orderKey,
            partKey,
            supplierKey,
            lineNumber,
            quantity,
            GenerateUtils.formatMoney(extendedPriceInCents),
            GenerateUtils.formatMoney(discountPercent),
            GenerateUtils.formatMoney(taxPercent),
            returnFlag,
            status,
            GenerateUtils.formatDate(shipDate),
            GenerateUtils.formatDate(commitDate),
            GenerateUtils.formatDate(receiptDate),
            shipInstructions,
            shipMode,
            comment
        )
    }

    override fun setParams(ps: PreparedStatement, rowIdx: Int) {
        val base = rowIdx * 17
        ps.setInt(base + 1, orderKey.toInt())
        ps.setInt(base + 2, partKey.toInt())
        ps.setInt(base + 3, supplierKey.toInt())
        ps.setInt(base + 4, lineNumber)
        ps.setFloat(base + 5, quantity.toFloat())
        ps.setFloat(base + 7, extendedPriceInCents.toFloat() / 100f)
        ps.setFloat(base + 8, discountPercent.toFloat() / 100f)
        ps.setFloat(base + 9, taxPercent.toFloat() / 100f)
        ps.setString(base + 10, returnFlag)
        ps.setString(base + 11, status)
        ps.setTimestamp(base + 12, Timestamp(shipDate.toLong()))
        ps.setTimestamp(base + 13, Timestamp(commitDate.toLong()))
        ps.setTimestamp(base + 14, Timestamp(receiptDate.toLong()))
        ps.setString(base + 15, shipInstructions)
        ps.setString(base + 16, shipMode)
        ps.setString(base + 17, comment)
    }

}