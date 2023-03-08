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
import io.trino.tpch.models.Supplier
import io.trino.tpch.random.*
import java.util.*

class SupplierGenerator @JvmOverloads constructor(
    scaleFactor: Double,
    part: Int,
    partCount: Int,
    distributions: Distributions = Distributions.getDefaultDistributions(),
    textPool: TextPool = TextPool.getDefaultTextPool()
) : ItemGenerator<Supplier?> {
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

    override fun iterator(): SupplierGeneratorIterator {
        return SupplierGeneratorIterator(
            distributions,
            textPool,
            GenerateUtils.calculateStartIndex(SCALE_BASE, scaleFactor, part, partCount),
            GenerateUtils.calculateRowCount(SCALE_BASE, scaleFactor, part, partCount)
        )
    }

    override fun getInsertStmt(rowCount: Int): String {
        val colCount = 7
        val rows = (0 until rowCount).map { rowIdx ->
            val tokens = (1..colCount).map { colIdx -> "$${rowIdx * colCount + colIdx}" }
            "(${tokens.joinToString(", ")})"
        }.joinToString(",\n\t")
        return "insert into supplier (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment) values\n\t$rows"
    }

    class SupplierGeneratorIterator constructor(
        distributions: Distributions,
        textPool: TextPool,
        private val startIndex: Long,
        private val rowCount: Long
    ) : AbstractIterator<Supplier?>() {
        private val addressRandom = RandomAlphaNumeric(706178559, ADDRESS_AVERAGE_LENGTH)
        private val nationKeyRandom: RandomBoundedInt
        private val phoneRandom = RandomPhoneNumber(884434366)
        private val accountBalanceRandom = RandomBoundedInt(962338209, ACCOUNT_BALANCE_MIN, ACCOUNT_BALANCE_MAX)
        private val commentRandom: RandomText
        private val bbbCommentRandom = RandomBoundedInt(202794285, 1, SCALE_BASE)
        private val bbbJunkRandom = RandomInt(263032577, 1)
        private val bbbOffsetRandom = RandomInt(715851524, 1)
        private val bbbTypeRandom = RandomBoundedInt(753643799, 0, 100)
        private var index: Long = 0

        init {
            nationKeyRandom = RandomBoundedInt(110356601, 0, distributions.nations.size() - 1)
            commentRandom = RandomText(1341315363, textPool, COMMENT_AVERAGE_LENGTH.toDouble())
            addressRandom.advanceRows(startIndex)
            nationKeyRandom.advanceRows(startIndex)
            phoneRandom.advanceRows(startIndex)
            accountBalanceRandom.advanceRows(startIndex)
            commentRandom.advanceRows(startIndex)
            bbbCommentRandom.advanceRows(startIndex)
            bbbJunkRandom.advanceRows(startIndex)
            bbbOffsetRandom.advanceRows(startIndex)
            bbbTypeRandom.advanceRows(startIndex)
        }

        override fun computeNext(): Supplier? {
            if (index >= rowCount) {
                return endOfData()
            }
            val supplier = makeSupplier(startIndex + index + 1)
            addressRandom.rowFinished()
            nationKeyRandom.rowFinished()
            phoneRandom.rowFinished()
            accountBalanceRandom.rowFinished()
            commentRandom.rowFinished()
            bbbCommentRandom.rowFinished()
            bbbJunkRandom.rowFinished()
            bbbOffsetRandom.rowFinished()
            bbbTypeRandom.rowFinished()
            index++
            return supplier
        }

        private fun makeSupplier(supplierKey: Long): Supplier {
            var comment = commentRandom.nextValue()

            // Add supplier complaints or commendation to the comment
            val bbbCommentRandomValue = bbbCommentRandom.nextValue()
            if (bbbCommentRandomValue <= BBB_COMMENTS_PER_SCALE_BASE) {
                val buffer = StringBuilder(comment)

                // select random place for BBB comment
                val noise = bbbJunkRandom.nextInt(0, comment.length - BBB_COMMENT_LENGTH)
                val offset = bbbOffsetRandom.nextInt(0, comment.length - (BBB_COMMENT_LENGTH + noise))

                // select complaint or recommendation
                val type: String
                type = if (bbbTypeRandom.nextValue() < BBB_COMPLAINT_PERCENT) {
                    BBB_COMPLAINT_TEXT
                } else {
                    BBB_RECOMMEND_TEXT
                }

                // write base text (e.g., "Customer ")
                buffer.replace(offset, offset + BBB_BASE_TEXT.length, BBB_BASE_TEXT)

                // write complaint or commendation text (e.g., "Complaints" or "Recommends")
                buffer.replace(
                    BBB_BASE_TEXT.length + offset + noise,
                    BBB_BASE_TEXT.length + offset + noise + type.length,
                    type
                )
                comment = buffer.toString()
            }
            val nationKey = nationKeyRandom.nextValue().toLong()
            return Supplier(
                supplierKey,
                supplierKey, String.format(Locale.ENGLISH, "Supplier#%09d", supplierKey),
                addressRandom.nextValue(),
                nationKey,
                phoneRandom.nextValue(nationKey),
                accountBalanceRandom.nextValue().toLong(),
                comment
            )
        }
    }

    companion object {
        const val SCALE_BASE = 10000
        private const val ACCOUNT_BALANCE_MIN = -99999
        private const val ACCOUNT_BALANCE_MAX = 999999
        private const val ADDRESS_AVERAGE_LENGTH = 25
        private const val COMMENT_AVERAGE_LENGTH = 63
        const val BBB_BASE_TEXT = "Customer "
        const val BBB_COMPLAINT_TEXT = "Complaints"
        const val BBB_RECOMMEND_TEXT = "Recommends"
        const val BBB_COMMENT_LENGTH = BBB_BASE_TEXT.length + BBB_COMPLAINT_TEXT.length
        const val BBB_COMMENTS_PER_SCALE_BASE = 10
        const val BBB_COMPLAINT_PERCENT = 50
    }
}