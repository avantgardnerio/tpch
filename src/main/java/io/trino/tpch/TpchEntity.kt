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
package io.trino.tpch

import org.joda.time.format.DateTimeFormat
import java.sql.PreparedStatement
import java.util.*

interface TpchEntity {
    val rowNumber: Long
    fun toLine(): String?
    fun setParams(ps: PreparedStatement, rowIdx: Int)

    fun convertDate(dt: Int): String? {
        val daysSinceEpoch = dt.toLong()
        val millisSinceEpoch = daysSinceEpoch * 24L * 60L * 60L * 1000L
        val date = Date(millisSinceEpoch)
        val pattern = "yyyy-MM-dd HH:mm:ss.SSS"
        val formatter = DateTimeFormat.forPattern(pattern)
        val formattedDateTime = formatter.print(date.time)
        return formattedDateTime
    }
}