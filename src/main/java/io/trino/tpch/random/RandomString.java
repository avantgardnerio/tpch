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

import io.trino.tpch.Distribution;

public class RandomString
        extends RandomInt
{
    private final Distribution distribution;

    public RandomString(long seed, Distribution distribution)
    {
        this(seed, distribution, 1);
    }

    public RandomString(long seed, Distribution distribution, int expectedRowCount)
    {
        super(seed, expectedRowCount);
        this.distribution = distribution;
    }

    public String nextValue()
    {
        return distribution.randomValue(this);
    }
}
