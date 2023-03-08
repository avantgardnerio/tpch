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
package io.trino.tpch.generators

import com.google.common.base.Preconditions
import com.google.common.collect.AbstractIterator
import io.trino.tpch.Distributions
import io.trino.tpch.GenerateUtils
import io.trino.tpch.TextPool
import io.trino.tpch.models.Customer
import io.trino.tpch.random.*
import java.util.*

class CustomerGenerator @JvmOverloads constructor(
    scaleFactor: Double,
    part: Int,
    partCount: Int,
    distributions: Distributions = Distributions.getDefaultDistributions(),
    textPool: TextPool = TextPool.getDefaultTextPool()
) : ItemGenerator<Customer?> {
    private val scaleFactor: Double
    private val part: Int
    private val partCount: Int
    private val distributions: Distributions
    private val textPool: TextPool

    init {
        Preconditions.checkArgument(scaleFactor > 0, "scaleFactor must be greater than 0")
        Preconditions.checkArgument(part >= 1, "part must be at least 1")
        Preconditions.checkArgument(part <= partCount, "part must be less than or equal to part count")
        this.scaleFactor = scaleFactor
        this.part = part
        this.partCount = partCount
        this.distributions = Objects.requireNonNull(distributions, "distributions is null")
        this.textPool = Objects.requireNonNull(textPool, "textPool is null")
    }

    override fun iterator(): CustomerGeneratorIterator {
        return CustomerGeneratorIterator(
            distributions,
            textPool,
            GenerateUtils.calculateStartIndex(SCALE_BASE, scaleFactor, part, partCount),
            GenerateUtils.calculateRowCount(SCALE_BASE, scaleFactor, part, partCount)
        )
    }

    override fun getInsertStmt(rowCount: Int): String {
        val colCount = 8
        val rows = (0 until rowCount).map { rowIdx ->
            val tokens = (1..colCount).map { colIdx -> "$${rowIdx * colCount + colIdx}" }
            "(${tokens.joinToString(", ")})"
        }.joinToString(",\n\t")
        return "insert into customer (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) values\n\t$rows"
    }

    class CustomerGeneratorIterator constructor(
        distributions: Distributions,
        textPool: TextPool,
        private val startIndex: Long,
        private val rowCount: Long
    ) : AbstractIterator<Customer?>() {
        private val addressRandom = RandomAlphaNumeric(881155353, ADDRESS_AVERAGE_LENGTH)
        private val nationKeyRandom: RandomBoundedInt
        private val phoneRandom = RandomPhoneNumber(1521138112)
        private val accountBalanceRandom = RandomBoundedInt(298370230, ACCOUNT_BALANCE_MIN, ACCOUNT_BALANCE_MAX)
        private val marketSegmentRandom: RandomString
        private val commentRandom: RandomText
        private var index: Long = 0

        init {
            nationKeyRandom = RandomBoundedInt(1489529863, 0, distributions.nations.size() - 1)
            marketSegmentRandom = RandomString(1140279430, distributions.marketSegments)
            commentRandom = RandomText(1335826707, textPool, COMMENT_AVERAGE_LENGTH.toDouble())
            addressRandom.advanceRows(startIndex)
            nationKeyRandom.advanceRows(startIndex)
            phoneRandom.advanceRows(startIndex)
            accountBalanceRandom.advanceRows(startIndex)
            marketSegmentRandom.advanceRows(startIndex)
            commentRandom.advanceRows(startIndex)
        }

        override fun computeNext(): Customer? {
            if (index >= rowCount) {
                return endOfData()
            }
            val customer = makeCustomer(startIndex + index + 1)
            addressRandom.rowFinished()
            nationKeyRandom.rowFinished()
            phoneRandom.rowFinished()
            accountBalanceRandom.rowFinished()
            marketSegmentRandom.rowFinished()
            commentRandom.rowFinished()
            index++
            return customer
        }

        private fun makeCustomer(customerKey: Long): Customer {
            val nationKey = nationKeyRandom.nextValue().toLong()
            return Customer(
                customerKey,
                customerKey, String.format(Locale.ENGLISH, "Customer#%09d", customerKey),
                addressRandom.nextValue(),
                nationKey,
                phoneRandom.nextValue(nationKey),
                accountBalanceRandom.nextValue().toLong(),
                marketSegmentRandom.nextValue(),
                commentRandom.nextValue()
            )
        }
    }

    companion object {
        const val SCALE_BASE = 150000
        private const val ACCOUNT_BALANCE_MIN = -99999
        private const val ACCOUNT_BALANCE_MAX = 999999
        private const val ADDRESS_AVERAGE_LENGTH = 25
        private const val COMMENT_AVERAGE_LENGTH = 73
    }
}