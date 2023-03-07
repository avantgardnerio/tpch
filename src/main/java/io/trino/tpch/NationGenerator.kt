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

import com.google.common.collect.AbstractIterator
import java.util.*

class NationGenerator @JvmOverloads constructor(
    distributions: Distributions = Distributions.getDefaultDistributions(),
    textPool: TextPool = TextPool.getDefaultTextPool()
) : Iterable<Nation?> {
    private val distributions: Distributions
    private val textPool: TextPool

    init {
        this.distributions = Objects.requireNonNull(distributions, "distributions is null")
        this.textPool = Objects.requireNonNull(textPool, "textPool is null")
    }

    override fun iterator(): NationGeneratorIterator {
        return NationGeneratorIterator(distributions.nations, textPool)
    }

    class NationGeneratorIterator constructor(private val nations: Distribution, textPool: TextPool) :
        AbstractIterator<Nation?>() {
        private val commentRandom: RandomText
        private var index = 0

        init {
            commentRandom = RandomText(606179079, textPool, COMMENT_AVERAGE_LENGTH.toDouble())
        }

        override fun computeNext(): Nation? {
            if (index >= nations.size()) {
                return endOfData()
            }
            val nation = Nation(
                index.toLong(),
                index.toLong(),
                nations.getValue(index),
                nations.getWeight(index).toLong(),
                commentRandom.nextValue()
            )
            commentRandom.rowFinished()
            index++
            return nation
        }
    }

    companion object {
        private const val COMMENT_AVERAGE_LENGTH = 72

        fun getInsertStmt(rowCount: Int): String {
            val colCount = 4;
            val rows = (0 until rowCount).map { rowIdx ->
                val tokens = (1..colCount).map { colIdx -> "$${rowIdx * colCount + colIdx}" }
                "(${tokens.joinToString(", ")})"
            }.joinToString(",\n\t")
            return "insert into nation (n_nationkey, n_name, n_regionkey, n_comment) values\n\t$rows"
        }
    }
}