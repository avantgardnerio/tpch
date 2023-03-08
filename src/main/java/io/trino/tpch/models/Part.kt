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

class Part(
    override val rowNumber: Long,
    @JvmField val partKey: Long,
    name: String,
    manufacturer: String,
    brand: String,
    type: String,
    @JvmField val size: Int,
    container: String,
    val retailPriceInCents: Long,
    comment: String
) : TpchEntity {
    @JvmField
    val name: String
    @JvmField
    val manufacturer: String
    @JvmField
    val brand: String
    @JvmField
    val type: String
    @JvmField
    val container: String
    @JvmField
    val comment: String

    init {
        this.name = Objects.requireNonNull(name, "name is null")
        this.manufacturer = Objects.requireNonNull(manufacturer, "manufacturer is null")
        this.brand = Objects.requireNonNull(brand, "brand is null")
        this.type = Objects.requireNonNull(type, "type is null")
        this.container = Objects.requireNonNull(container, "container is null")
        this.comment = Objects.requireNonNull(comment, "comment is null")
    }

    fun getRetailPrice(): Double {
        return retailPriceInCents / 100.0
    }

    override fun toLine(): String? {
        return String.format(
            Locale.ENGLISH,
            "%d|%s|%s|%s|%s|%d|%s|%s|%s|",
            partKey,
            name,
            manufacturer,
            brand,
            type,
            size,
            container,
            GenerateUtils.formatMoney(retailPriceInCents),
            comment
        )
    }

    override fun setParams(ps: PreparedStatement, rowIdx: Int) {
        val base = rowIdx * 9
        ps.setInt(base + 1, partKey.toInt())
        ps.setString(base + 2, name)
        ps.setString(base + 3, manufacturer)
        ps.setString(base + 4, brand)
        ps.setString(base + 5, type)
        ps.setInt(base + 6, size)
        ps.setString(base + 7, container)
        ps.setFloat(base + 8, retailPriceInCents.toFloat() / 100f)
        ps.setString(base + 9, comment)
    }
}