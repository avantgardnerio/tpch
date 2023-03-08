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

import com.google.common.collect.AbstractIterator
import io.trino.tpch.*
import io.trino.tpch.models.Region
import io.trino.tpch.random.RandomText
import java.util.*

class RegionGenerator @JvmOverloads constructor(
    distributions: Distributions = Distributions.getDefaultDistributions(),
    textPool: TextPool = TextPool.getDefaultTextPool()
) : ItemGenerator<Region?> {
    private val distributions: Distributions
    private val textPool: TextPool

    init {
        this.distributions = Objects.requireNonNull(distributions, "distributions is null")
        this.textPool = Objects.requireNonNull(textPool, "textPool is null")
    }

    override fun iterator(): RegionGeneratorIterator {
        return RegionGeneratorIterator(distributions.regions, textPool)
    }

    class RegionGeneratorIterator constructor(private val regions: Distribution, textPool: TextPool) :
        AbstractIterator<Region?>() {
        private val commentRandom: RandomText
        private var index = 0

        init {
            commentRandom = RandomText(
                1500869201,
                textPool,
                COMMENT_AVERAGE_LENGTH.toDouble()
            )
        }

        override fun computeNext(): Region? {
            if (index >= regions.size()) {
                return endOfData()
            }
            val region = Region(
                index.toLong(),
                index.toLong(),
                regions.getValue(index),
                commentRandom.nextValue()
            )
            commentRandom.rowFinished()
            index++
            return region
        }
    }

    override fun getInsertStmt(rowCount: Int): String {
        val colCount = 3;
        val rows = (0 until rowCount).map { rowIdx ->
            val tokens = (1..colCount).map { colIdx -> "$${rowIdx * colCount + colIdx}" }
            "(${tokens.joinToString(", ")})"
        }.joinToString(",\n\t")
        return "insert into region (r_regionkey, r_name, r_comment) values\n\t$rows"
    }

    companion object {
        private const val COMMENT_AVERAGE_LENGTH = 72
    }
}