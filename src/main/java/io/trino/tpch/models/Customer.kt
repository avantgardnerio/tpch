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
import java.util.*

class Customer(
    override val rowNumber: Long,
    @JvmField val customerKey: Long,
    name: String,
    address: String,
    @JvmField val nationKey: Long,
    phone: String,
    val accountBalanceInCents: Long,
    marketSegment: String,
    comment: String
) : TpchEntity {
    @JvmField
    val name: String

    @JvmField
    val address: String

    @JvmField
    val phone: String

    @JvmField
    val marketSegment: String

    @JvmField
    val comment: String

    init {
        this.name = Objects.requireNonNull(name, "name is null")
        this.address = Objects.requireNonNull(address, "address is null")
        this.phone = Objects.requireNonNull(phone, "phone is null")
        this.marketSegment = Objects.requireNonNull(marketSegment, "marketSegment is null")
        this.comment = Objects.requireNonNull(comment, "comment is null")
    }

    fun getAccountBalance(): Double {
        return accountBalanceInCents / 100.0
    }

    override fun toLine(): String? {
        return String.format(
            Locale.ENGLISH,
            "%d|%s|%s|%d|%s|%s|%s|%s|",
            customerKey,
            name,
            address,
            nationKey,
            phone,
            GenerateUtils.formatMoney(accountBalanceInCents),
            marketSegment,
            comment
        )
    }

    override fun setParams(ps: PreparedStatement, rowIdx: Int) {
        val base = rowIdx * 8
        ps.setInt(base + 1, customerKey.toInt())
        ps.setString(base + 2, name)
        ps.setString(base + 3, address)
        ps.setInt(base + 4, nationKey.toInt())
        ps.setString(base + 5, phone)
        ps.setFloat(base + 6, accountBalanceInCents.toFloat() / 100.0f)
        ps.setString(base + 7, marketSegment)
        ps.setString(base + 8, comment)
    }
}