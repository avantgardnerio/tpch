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
import io.trino.tpch.generators.OrderGenerator.Companion.createLineCountRandom
import io.trino.tpch.generators.OrderGenerator.Companion.createOrderDateRandom
import io.trino.tpch.generators.OrderGenerator.Companion.makeOrderKey
import io.trino.tpch.generators.PartGenerator.Companion.calculatePartPrice
import io.trino.tpch.generators.PartSupplierGenerator.Companion.selectPartSupplier
import io.trino.tpch.models.LineItem
import io.trino.tpch.random.RandomBoundedInt
import io.trino.tpch.random.RandomBoundedLong
import io.trino.tpch.random.RandomString
import io.trino.tpch.random.RandomText
import java.util.*

class LineItemGenerator @JvmOverloads constructor(
    scaleFactor: Double,
    part: Int,
    partCount: Int,
    distributions: Distributions = Distributions.getDefaultDistributions(),
    textPool: TextPool = TextPool.getDefaultTextPool()
) : ItemGenerator<LineItem?> {
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

    override fun getInsertStmt(rowCount: Int): String {
        val colCount = 16
        val rows = (0 until rowCount).map { rowIdx ->
            val tokens = (1..colCount).map { colIdx -> "$${rowIdx * colCount + colIdx}" }
            "(${tokens.joinToString(", ")})"
        }.joinToString(",\n\t")
        return "insert into lineitem (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment) values\n\t$rows"
    }

    override fun iterator(): LineItemGeneratorIterator {
        return LineItemGeneratorIterator(
            distributions,
            textPool,
            scaleFactor,
            GenerateUtils.calculateStartIndex(OrderGenerator.SCALE_BASE, scaleFactor, part, partCount),
            GenerateUtils.calculateRowCount(OrderGenerator.SCALE_BASE, scaleFactor, part, partCount)
        )
    }

    class LineItemGeneratorIterator constructor(
        distributions: Distributions,
        textPool: TextPool,
        private val scaleFactor: Double,
        private val startIndex: Long,
        private val rowCount: Long
    ) : AbstractIterator<LineItem?>() {
        private val orderDateRandom = createOrderDateRandom()
        private val lineCountRandom = createLineCountRandom()
        private val quantityRandom = createQuantityRandom()
        private val discountRandom = createDiscountRandom()
        private val taxRandom = createTaxRandom()
        private val linePartKeyRandom: RandomBoundedLong
        private val supplierNumberRandom = RandomBoundedInt(2095021727, 0, 3, OrderGenerator.LINE_COUNT_MAX)
        private val shipDateRandom = createShipDateRandom()
        private val commitDateRandom =
            RandomBoundedInt(904914315, COMMIT_DATE_MIN, COMMIT_DATE_MAX, OrderGenerator.LINE_COUNT_MAX)
        private val receiptDateRandom =
            RandomBoundedInt(373135028, RECEIPT_DATE_MIN, RECEIPT_DATE_MAX, OrderGenerator.LINE_COUNT_MAX)
        private val returnedFlagRandom: RandomString
        private val shipInstructionsRandom: RandomString
        private val shipModeRandom: RandomString
        private val commentRandom: RandomText
        private var index: Long = 0
        private var orderDate: Int
        private var lineCount: Int
        private var lineNumber = 0

        init {
            returnedFlagRandom = RandomString(717419739, distributions.returnFlags, OrderGenerator.LINE_COUNT_MAX)
            shipInstructionsRandom =
                RandomString(1371272478, distributions.shipInstructions, OrderGenerator.LINE_COUNT_MAX)
            shipModeRandom = RandomString(675466456, distributions.shipModes, OrderGenerator.LINE_COUNT_MAX)
            commentRandom =
                RandomText(1095462486, textPool, COMMENT_AVERAGE_LENGTH.toDouble(), OrderGenerator.LINE_COUNT_MAX)
            linePartKeyRandom = createPartKeyRandom(scaleFactor)
            orderDateRandom.advanceRows(startIndex)
            lineCountRandom.advanceRows(startIndex)
            quantityRandom.advanceRows(startIndex)
            discountRandom.advanceRows(startIndex)
            taxRandom.advanceRows(startIndex)
            linePartKeyRandom.advanceRows(startIndex)
            supplierNumberRandom.advanceRows(startIndex)
            shipDateRandom.advanceRows(startIndex)
            commitDateRandom.advanceRows(startIndex)
            receiptDateRandom.advanceRows(startIndex)
            returnedFlagRandom.advanceRows(startIndex)
            shipInstructionsRandom.advanceRows(startIndex)
            shipModeRandom.advanceRows(startIndex)
            commentRandom.advanceRows(startIndex)

            // generate information for initial order
            orderDate = orderDateRandom.nextValue()
            lineCount = lineCountRandom.nextValue() - 1
        }

        override fun computeNext(): LineItem? {
            if (index >= rowCount) {
                return endOfData()
            }
            val lineitem = makeLineitem(startIndex + index + 1)
            lineNumber++

            // advance next row only when all lines for the order have been produced
            if (lineNumber > lineCount) {
                orderDateRandom.rowFinished()
                lineCountRandom.rowFinished()
                quantityRandom.rowFinished()
                discountRandom.rowFinished()
                taxRandom.rowFinished()
                linePartKeyRandom.rowFinished()
                supplierNumberRandom.rowFinished()
                shipDateRandom.rowFinished()
                commitDateRandom.rowFinished()
                receiptDateRandom.rowFinished()
                returnedFlagRandom.rowFinished()
                shipInstructionsRandom.rowFinished()
                shipModeRandom.rowFinished()
                commentRandom.rowFinished()
                index++

                // generate information for next order
                lineCount = lineCountRandom.nextValue() - 1
                orderDate = orderDateRandom.nextValue()
                lineNumber = 0
            }
            return lineitem
        }

        private fun makeLineitem(orderIndex: Long): LineItem {
            val orderKey = makeOrderKey(orderIndex)
            val quantity = quantityRandom.nextValue()
            val discount = discountRandom.nextValue()
            val tax = taxRandom.nextValue()
            val partKey = linePartKeyRandom.nextValue()
            val supplierNumber = supplierNumberRandom.nextValue()
            val supplierKey = selectPartSupplier(partKey, supplierNumber.toLong(), scaleFactor)
            val partPrice = calculatePartPrice(partKey)
            val extendedPrice = partPrice * quantity
            var shipDate = shipDateRandom.nextValue()
            shipDate += orderDate
            var commitDate = commitDateRandom.nextValue()
            commitDate += orderDate
            var receiptDate = receiptDateRandom.nextValue()
            receiptDate += shipDate
            val returnedFlag: String
            returnedFlag = if (GenerateUtils.isInPast(receiptDate)) {
                returnedFlagRandom.nextValue()
            } else {
                "N"
            }
            val status: String
            status = if (GenerateUtils.isInPast(shipDate)) {
                "F"
            } else {
                "O"
            }
            val shipInstructions = shipInstructionsRandom.nextValue()
            val shipMode = shipModeRandom.nextValue()
            val comment = commentRandom.nextValue()
            return LineItem(
                orderIndex,
                orderKey,
                partKey,
                supplierKey,
                lineNumber + 1,
                quantity.toLong(),
                extendedPrice,
                discount.toLong(),
                tax.toLong(),
                returnedFlag,
                status,
                GenerateUtils.toEpochDate(shipDate),
                GenerateUtils.toEpochDate(commitDate),
                GenerateUtils.toEpochDate(receiptDate),
                shipInstructions,
                shipMode,
                comment
            )
        }
    }

    companion object {
        private const val QUANTITY_MIN = 1
        private const val QUANTITY_MAX = 50
        private const val TAX_MIN = 0
        private const val TAX_MAX = 8
        private const val DISCOUNT_MIN = 0
        private const val DISCOUNT_MAX = 10
        private const val PART_KEY_MIN = 1
        private const val SHIP_DATE_MIN = 1
        private const val SHIP_DATE_MAX = 121
        private const val COMMIT_DATE_MIN = 30
        private const val COMMIT_DATE_MAX = 90
        private const val RECEIPT_DATE_MIN = 1
        private const val RECEIPT_DATE_MAX = 30
        const val ITEM_SHIP_DAYS = SHIP_DATE_MAX + RECEIPT_DATE_MAX
        private const val COMMENT_AVERAGE_LENGTH = 27
        fun createQuantityRandom(): RandomBoundedInt {
            return RandomBoundedInt(209208115, QUANTITY_MIN, QUANTITY_MAX, OrderGenerator.LINE_COUNT_MAX)
        }

        fun createDiscountRandom(): RandomBoundedInt {
            return RandomBoundedInt(554590007, DISCOUNT_MIN, DISCOUNT_MAX, OrderGenerator.LINE_COUNT_MAX)
        }

        fun createTaxRandom(): RandomBoundedInt {
            return RandomBoundedInt(721958466, TAX_MIN, TAX_MAX, OrderGenerator.LINE_COUNT_MAX)
        }

        fun createPartKeyRandom(scaleFactor: Double): RandomBoundedLong {
            return RandomBoundedLong(
                1808217256,
                scaleFactor >= 30000,
                PART_KEY_MIN.toLong(),
                (PartGenerator.SCALE_BASE * scaleFactor).toLong(),
                OrderGenerator.LINE_COUNT_MAX
            )
        }

        fun createShipDateRandom(): RandomBoundedInt {
            return RandomBoundedInt(1769349045, SHIP_DATE_MIN, SHIP_DATE_MAX, OrderGenerator.LINE_COUNT_MAX)
        }
    }
}