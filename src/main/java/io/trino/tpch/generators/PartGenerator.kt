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
import io.trino.tpch.models.Part
import io.trino.tpch.random.RandomBoundedInt
import io.trino.tpch.random.RandomString
import io.trino.tpch.random.RandomStringSequence
import io.trino.tpch.random.RandomText
import java.util.*

class PartGenerator @JvmOverloads constructor(
    scaleFactor: Double,
    part: Int,
    partCount: Int,
    distributions: Distributions = Distributions.getDefaultDistributions(),
    textPool: TextPool = TextPool.getDefaultTextPool()
) : ItemGenerator<Part?> {
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

    override fun iterator(): PartGeneratorIterator {
        return PartGeneratorIterator(
            distributions,
            textPool,
            GenerateUtils.calculateStartIndex(SCALE_BASE, scaleFactor, part, partCount),
            GenerateUtils.calculateRowCount(SCALE_BASE, scaleFactor, part, partCount)
        )
    }

    override fun getInsertStmt(rowCount: Int): String {
        val colCount = 9
        val rows = (0 until rowCount).map { rowIdx ->
            val tokens = (1..colCount).map { colIdx -> "$${rowIdx * colCount + colIdx}" }
            "(${tokens.joinToString(", ")})"
        }.joinToString(",\n\t")
        return "insert into part (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment) values\n\t$rows"
    }

    class PartGeneratorIterator constructor(
        distributions: Distributions,
        textPool: TextPool,
        private val startIndex: Long,
        private val rowCount: Long
    ) : AbstractIterator<Part?>() {
        private val nameRandom: RandomStringSequence
        private val manufacturerRandom: RandomBoundedInt
        private val brandRandom: RandomBoundedInt
        private val typeRandom: RandomString
        private val sizeRandom: RandomBoundedInt
        private val containerRandom: RandomString
        private val commentRandom: RandomText
        private var index: Long = 0

        init {
            nameRandom = RandomStringSequence(709314158, NAME_WORDS, distributions.partColors)
            manufacturerRandom = RandomBoundedInt(1, MANUFACTURER_MIN, MANUFACTURER_MAX)
            brandRandom = RandomBoundedInt(46831694, BRAND_MIN, BRAND_MAX)
            typeRandom = RandomString(1841581359, distributions.partTypes)
            sizeRandom = RandomBoundedInt(1193163244, SIZE_MIN, SIZE_MAX)
            containerRandom = RandomString(727633698, distributions.partContainers)
            commentRandom = RandomText(804159733, textPool, COMMENT_AVERAGE_LENGTH.toDouble())
            nameRandom.advanceRows(startIndex)
            manufacturerRandom.advanceRows(startIndex)
            brandRandom.advanceRows(startIndex)
            typeRandom.advanceRows(startIndex)
            sizeRandom.advanceRows(startIndex)
            containerRandom.advanceRows(startIndex)
            commentRandom.advanceRows(startIndex)
        }

        override fun computeNext(): Part? {
            if (index >= rowCount) {
                return endOfData()
            }
            val part = makePart(startIndex + index + 1)
            nameRandom.rowFinished()
            manufacturerRandom.rowFinished()
            brandRandom.rowFinished()
            typeRandom.rowFinished()
            sizeRandom.rowFinished()
            containerRandom.rowFinished()
            commentRandom.rowFinished()
            index++
            return part
        }

        private fun makePart(partKey: Long): Part {
            val name = nameRandom.nextValue()
            val manufacturer = manufacturerRandom.nextValue()
            val brand = manufacturer * 10 + brandRandom.nextValue()
            return Part(
                partKey,
                partKey,
                name,
                String.format(Locale.ENGLISH, "Manufacturer#%d", manufacturer),
                String.format(Locale.ENGLISH, "Brand#%d", brand),
                typeRandom.nextValue(),
                sizeRandom.nextValue(),
                containerRandom.nextValue(),
                calculatePartPrice(partKey),
                commentRandom.nextValue()
            )
        }
    }

    companion object {
        const val SCALE_BASE = 200000
        private const val NAME_WORDS = 5
        private const val MANUFACTURER_MIN = 1
        private const val MANUFACTURER_MAX = 5
        private const val BRAND_MIN = 1
        private const val BRAND_MAX = 5
        private const val SIZE_MIN = 1
        private const val SIZE_MAX = 50
        private const val COMMENT_AVERAGE_LENGTH = 14
        @JvmStatic
        fun calculatePartPrice(p: Long): Long {
            var price: Long = 90000

            // limit contribution to $200
            price += p / 10 % 20001
            price += p % 1000 * 100
            return price
        }
    }
}