/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java.summarize;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Generic Column Summary for Numeric Types
 *
 * @param <T> the numeric type e.g. Integer, DoubleValue
 */
@PublicEvolving
public class NumericColumnSummary<T> extends ColumnSummary implements java.io.Serializable {

	private static final long serialVersionUID = 1L;

	private final long nonMissingCount; // count of elements that are NOT null, NaN, or Infinite
	private final long nullCount;
	private final long nanCount; // always zero for types like Short, Integer, Long
	private final long infinityCount; // always zero for types like Short, Integer, Long

	private final T min;
	private final T max;
	private final T sum;

	private final Double mean;
	private final Double variance;
	private final Double standardDeviation;

	public NumericColumnSummary(long nonMissingCount, long nullCount, long nanCount, long infinityCount, T min, T max, T sum, Double mean, Double variance, Double standardDeviation) {
		this.nonMissingCount = nonMissingCount;
		this.nullCount = nullCount;
		this.nanCount = nanCount;
		this.infinityCount = infinityCount;
		this.min = min;
		this.max = max;
		this.sum = sum;
		this.mean = mean;
		this.variance = variance;
		this.standardDeviation = standardDeviation;
	}

	/**
	 * The number of "missing" values where "missing" is defined as null, NaN, or Infinity.
	 *
	 * These values are ignored in some calculations like mean, variance, and standardDeviation.
	 */
	public long getMissingCount() {
		return nullCount + nanCount + infinityCount;
	}

	public long getNonMissingCount() {
		return nonMissingCount;
	}

	/**
	 * The number of non-null values in this column
	 */
	@Override
	public long getNonNullCount() {
		return nonMissingCount + nanCount + infinityCount;
	}

	@Override
	public long getNullCount() {
		return nullCount;
	}

	public long getNanCount() {
		return nanCount;
	}

	public long getInfinityCount() {
		return infinityCount;
	}

	public T getMin() {
		return min;
	}

	public T getMax() {
		return max;
	}

	public T getSum() {
		return sum;
	}

	public Double getMean() {
		return mean;
	}

	public Double getVariance() {
		return variance;
	}

	public Double getStandardDeviation() {
		return standardDeviation;
	}

	@Override
	public String toString() {
		return "NumericColumnSummary{" +
			"totalCount=" + getTotalCount() +
			", nullCount=" + nullCount +
			", nonNullCount=" + getNonNullCount() +
			", missingCount=" + getMissingCount() +
			", nonMissingCount=" + nonMissingCount +
			", nanCount=" + nanCount +
			", infinityCount=" + infinityCount +
			", min=" + min +
			", max=" + max +
			", sum=" + sum +
			", mean=" + mean +
			", variance=" + variance +
			", standardDeviation=" + standardDeviation +
			'}';
	}
}
