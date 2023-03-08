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
import io.trino.tpch.GenerateUtils
import io.trino.tpch.TextPool
import io.trino.tpch.models.PartSupplier
import io.trino.tpch.random.RandomBoundedInt
import io.trino.tpch.random.RandomText
import java.util.*

class PartSupplierGenerator @JvmOverloads constructor(
    scaleFactor: Double,
    part: Int,
    partCount: Int,
    textPool: TextPool = TextPool.getDefaultTextPool()
) : ItemGenerator<PartSupplier?> {
    private val scaleFactor: Double
    private val part: Int
    private val partCount: Int
    private val textPool: TextPool

    init {
        Preconditions.checkArgument(scaleFactor > 0, "scaleFactor must be greater than 0")
        Preconditions.checkArgument(part >= 1, "part must be at least 1")
        Preconditions.checkArgument(part <= partCount, "part must be less than or equal to part count")
        this.scaleFactor = scaleFactor
        this.part = part
        this.partCount = partCount
        this.textPool = Objects.requireNonNull(textPool, "textPool is null")
    }

    override fun iterator(): PartSupplierGeneratorIterator {
        return PartSupplierGeneratorIterator(
            textPool,
            scaleFactor,
            GenerateUtils.calculateStartIndex(PartGenerator.SCALE_BASE, scaleFactor, part, partCount),
            GenerateUtils.calculateRowCount(PartGenerator.SCALE_BASE, scaleFactor, part, partCount)
        )
    }

    override fun getInsertStmt(rowCount: Int): String {
        val colCount = 5
        val rows = (0 until rowCount).map { rowIdx ->
            val tokens = (1..colCount).map { colIdx -> "$${rowIdx * colCount + colIdx}" }
            "(${tokens.joinToString(", ")})"
        }.joinToString(",\n\t")
        return "insert into partsupp (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment) values\n\t$rows"
    }

    class PartSupplierGeneratorIterator constructor(
        textPool: TextPool,
        private val scaleFactor: Double,
        private val startIndex: Long,
        private val rowCount: Long
    ) : AbstractIterator<PartSupplier?>() {
        private val availableQuantityRandom: RandomBoundedInt
        private val supplyCostRandom: RandomBoundedInt
        private val commentRandom: RandomText
        private var index: Long = 0
        private var partSupplierNumber = 0

        init {
            availableQuantityRandom =
                RandomBoundedInt(1671059989, AVAILABLE_QUANTITY_MIN, AVAILABLE_QUANTITY_MAX, SUPPLIERS_PER_PART)
            supplyCostRandom = RandomBoundedInt(1051288424, SUPPLY_COST_MIN, SUPPLY_COST_MAX, SUPPLIERS_PER_PART)
            commentRandom = RandomText(1961692154, textPool, COMMENT_AVERAGE_LENGTH.toDouble(), SUPPLIERS_PER_PART)
            availableQuantityRandom.advanceRows(startIndex)
            supplyCostRandom.advanceRows(startIndex)
            commentRandom.advanceRows(startIndex)
        }

        override fun computeNext(): PartSupplier? {
            if (index >= rowCount) {
                return endOfData()
            }
            val partSupplier = makePartSupplier(startIndex + index + 1)
            partSupplierNumber++

            // advance next row only when all lines for the order have been produced
            if (partSupplierNumber >= SUPPLIERS_PER_PART) {
                availableQuantityRandom.rowFinished()
                supplyCostRandom.rowFinished()
                commentRandom.rowFinished()
                index++
                partSupplierNumber = 0
            }
            return partSupplier
        }

        private fun makePartSupplier(partKey: Long): PartSupplier {
            return PartSupplier(
                partKey,
                partKey,
                selectPartSupplier(partKey, partSupplierNumber.toLong(), scaleFactor),
                availableQuantityRandom.nextValue(),
                supplyCostRandom.nextValue().toLong(),
                commentRandom.nextValue()
            )
        }
    }

    companion object {
        private const val SUPPLIERS_PER_PART = 4
        private const val AVAILABLE_QUANTITY_MIN = 1
        private const val AVAILABLE_QUANTITY_MAX = 9999
        private const val SUPPLY_COST_MIN = 100
        private const val SUPPLY_COST_MAX = 100000
        private const val COMMENT_AVERAGE_LENGTH = 124
        @JvmStatic
        fun selectPartSupplier(partKey: Long, supplierNumber: Long, scaleFactor: Double): Long {
            val supplierCount = (SupplierGenerator.SCALE_BASE * scaleFactor).toLong()
            return (partKey + supplierNumber * (supplierCount / SUPPLIERS_PER_PART + (partKey - 1) / supplierCount)) % supplierCount + 1
        }
    }
}