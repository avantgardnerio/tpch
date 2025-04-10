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
package io.trino.tpch.random;

import io.trino.tpch.AbstractRandomInt;
import io.trino.tpch.TextPool;

public class RandomText
        extends AbstractRandomInt
{
    private static final double LOW_LENGTH_MULTIPLIER = 0.4;
    private static final double HIGH_LENGTH_MULTIPLIER = 1.6;

    private final TextPool textPool;
    private final int minLength;
    private final int maxLength;

    public RandomText(long seed, TextPool textPool, double averageTextLength)
    {
        this(seed, textPool, averageTextLength, 1);
    }

    public RandomText(long seed, TextPool textPool, double averageTextLength, int expectedRowCount)
    {
        super(seed, expectedRowCount * 2);
        this.textPool = textPool;
        this.minLength = (int) (averageTextLength * LOW_LENGTH_MULTIPLIER);
        this.maxLength = (int) (averageTextLength * HIGH_LENGTH_MULTIPLIER);
    }

    public String nextValue()
    {
        int offset = nextInt(0, textPool.size() - maxLength);
        int length = nextInt(minLength, maxLength);
        return textPool.getText(offset, offset + length);
    }
}
