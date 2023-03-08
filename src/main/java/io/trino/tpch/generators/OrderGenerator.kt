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
import io.trino.tpch.generators.PartGenerator.Companion.calculatePartPrice
import io.trino.tpch.models.Order
import io.trino.tpch.random.RandomBoundedInt
import io.trino.tpch.random.RandomBoundedLong
import io.trino.tpch.random.RandomString
import io.trino.tpch.random.RandomText
import java.util.*

class OrderGenerator @JvmOverloads constructor(
    scaleFactor: Double,
    part: Int,
    partCount: Int,
    distributions: Distributions = Distributions.getDefaultDistributions(),
    textPool: TextPool = TextPool.getDefaultTextPool()
) : ItemGenerator<Order?> {
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

    override fun iterator(): OrderGeneratorIterator {
        return OrderGeneratorIterator(
            distributions,
            textPool,
            scaleFactor,
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
        return "insert into orders (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment) values\n\t$rows"
    }

    class OrderGeneratorIterator constructor(
        distributions: Distributions,
        textPool: TextPool,
        scaleFactor: Double,
        private val startIndex: Long,
        private val rowCount: Long
    ) : AbstractIterator<Order?>() {
        private val orderDateRandom = createOrderDateRandom()
        private val lineCountRandom = createLineCountRandom()
        private val customerKeyRandom: RandomBoundedLong
        private val orderPriorityRandom: RandomString
        private val clerkRandom: RandomBoundedInt
        private val commentRandom: RandomText
        private val lineQuantityRandom = LineItemGenerator.createQuantityRandom()
        private val lineDiscountRandom = LineItemGenerator.createDiscountRandom()
        private val lineTaxRandom = LineItemGenerator.createTaxRandom()
        private val linePartKeyRandom: RandomBoundedLong
        private val lineShipDateRandom = LineItemGenerator.createShipDateRandom()
        private val maxCustomerKey: Long
        private var index: Long = 0

        init {
            clerkRandom =
                RandomBoundedInt(1171034773, 1, Math.max((scaleFactor * CLERK_SCALE_BASE).toInt(), CLERK_SCALE_BASE))
            maxCustomerKey = (CustomerGenerator.SCALE_BASE * scaleFactor).toLong()
            customerKeyRandom = RandomBoundedLong(851767375, scaleFactor >= 30000, 1, maxCustomerKey)
            orderPriorityRandom = RandomString(591449447, distributions.orderPriorities)
            commentRandom = RandomText(276090261, textPool, COMMENT_AVERAGE_LENGTH.toDouble())
            linePartKeyRandom = LineItemGenerator.createPartKeyRandom(scaleFactor)
            orderDateRandom.advanceRows(startIndex)
            lineCountRandom.advanceRows(startIndex)
            customerKeyRandom.advanceRows(startIndex)
            orderPriorityRandom.advanceRows(startIndex)
            clerkRandom.advanceRows(startIndex)
            commentRandom.advanceRows(startIndex)
            lineQuantityRandom.advanceRows(startIndex)
            lineDiscountRandom.advanceRows(startIndex)
            lineShipDateRandom.advanceRows(startIndex)
            lineTaxRandom.advanceRows(startIndex)
            linePartKeyRandom.advanceRows(startIndex)
        }

        override fun computeNext(): Order? {
            if (index >= rowCount) {
                return endOfData()
            }
            val order = makeOrder(startIndex + index + 1)
            orderDateRandom.rowFinished()
            lineCountRandom.rowFinished()
            customerKeyRandom.rowFinished()
            orderPriorityRandom.rowFinished()
            clerkRandom.rowFinished()
            commentRandom.rowFinished()
            lineQuantityRandom.rowFinished()
            lineDiscountRandom.rowFinished()
            lineShipDateRandom.rowFinished()
            lineTaxRandom.rowFinished()
            linePartKeyRandom.rowFinished()
            index++
            return order
        }

        private fun makeOrder(index: Long): Order {
            val orderKey = makeOrderKey(index)
            val orderDate = orderDateRandom.nextValue()

            // generate customer key, taking into account customer mortality rate
            var customerKey = customerKeyRandom.nextValue()
            var delta = 1
            while (customerKey % CUSTOMER_MORTALITY == 0L) {
                customerKey += delta.toLong()
                customerKey = Math.min(customerKey, maxCustomerKey)
                delta *= -1
            }
            var totalPrice: Long = 0
            var shippedCount = 0
            val lineCount = lineCountRandom.nextValue()
            for (lineNumber in 0 until lineCount) {
                val quantity = lineQuantityRandom.nextValue()
                val discount = lineDiscountRandom.nextValue()
                val tax = lineTaxRandom.nextValue()
                val partKey = linePartKeyRandom.nextValue()
                val partPrice = calculatePartPrice(partKey)
                val extendedPrice = partPrice * quantity
                val discountedPrice = extendedPrice * (100 - discount)
                totalPrice += discountedPrice / 100 * (100 + tax) / 100
                var shipDate = lineShipDateRandom.nextValue()
                shipDate += orderDate
                if (GenerateUtils.isInPast(shipDate)) {
                    shippedCount++
                }
            }
            val orderStatus: Char
            orderStatus = if (shippedCount == lineCount) {
                'F'
            } else if (shippedCount > 0) {
                'P'
            } else {
                'O'
            }
            return Order(
                index,
                orderKey,
                customerKey,
                orderStatus,
                totalPrice,
                GenerateUtils.toEpochDate(orderDate),
                orderPriorityRandom.nextValue(), String.format(Locale.ENGLISH, "Clerk#%09d", clerkRandom.nextValue()),
                0,
                commentRandom.nextValue()
            )
        }
    }

    companion object {
        const val SCALE_BASE = 1500000

        // portion with have no orders
        const val CUSTOMER_MORTALITY = 3
        private const val ORDER_DATE_MIN = GenerateUtils.MIN_GENERATE_DATE
        private const val ORDER_DATE_MAX =
            ORDER_DATE_MIN + (GenerateUtils.TOTAL_DATE_RANGE - LineItemGenerator.ITEM_SHIP_DAYS - 1)
        private const val CLERK_SCALE_BASE = 1000
        private const val LINE_COUNT_MIN = 1
        const val LINE_COUNT_MAX = 7
        private const val COMMENT_AVERAGE_LENGTH = 49
        private const val ORDER_KEY_SPARSE_BITS = 2
        private const val ORDER_KEY_SPARSE_KEEP = 3
        @JvmStatic
        fun createLineCountRandom(): RandomBoundedInt {
            return RandomBoundedInt(1434868289, LINE_COUNT_MIN, LINE_COUNT_MAX)
        }

        @JvmStatic
        fun createOrderDateRandom(): RandomBoundedInt {
            return RandomBoundedInt(1066728069, ORDER_DATE_MIN, ORDER_DATE_MAX)
        }

        @JvmStatic
        fun makeOrderKey(orderIndex: Long): Long {
            val lowBits = orderIndex and ((1 shl ORDER_KEY_SPARSE_KEEP) - 1).toLong()
            var ok = orderIndex
            ok = ok shr ORDER_KEY_SPARSE_KEEP
            ok = ok shl ORDER_KEY_SPARSE_BITS
            ok = ok shl ORDER_KEY_SPARSE_KEEP
            ok += lowBits
            return ok
        }
    }
}