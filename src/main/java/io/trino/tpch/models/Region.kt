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

import io.trino.tpch.TpchEntity
import java.sql.PreparedStatement
import java.util.*

class Region(override val rowNumber: Long, @JvmField val regionKey: Long, name: String, comment: String) : TpchEntity {
    @JvmField
    val name: String

    @JvmField
    val comment: String

    init {
        this.name = Objects.requireNonNull(name, "name is null")
        this.comment = Objects.requireNonNull(comment, "comment is null")
    }

    override fun toLine(): String {
        return String.format(Locale.ENGLISH, "%d|%s|%s|", regionKey, name, comment)
    }

    override fun setParams(ps: PreparedStatement, rowIdx: Int) {
        val base = rowIdx * 3
        ps.setInt(base + 1, regionKey.toInt())
        ps.setString(base + 2, name)
        ps.setString(base + 3, comment)
    }
}