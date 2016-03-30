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
 * Summary for a column of Strings
 */
@PublicEvolving
public class StringColumnSummary extends ColumnSummary {

	private long nonNullCount;
	private long nullCount;
	private long empytCount;
	private Integer minStringLength;
	private Integer maxStringLength;
	private Double meanLength;

	public StringColumnSummary(long nonNullCount, long nullCount, long empytCount, Integer minStringLength, Integer maxStringLength, Double meanLength) {
		this.nonNullCount = nonNullCount;
		this.nullCount = nullCount;
		this.empytCount = empytCount;
		this.minStringLength = minStringLength;
		this.maxStringLength = maxStringLength;
		this.meanLength = meanLength;
	}

	@Override
	public long getNonNullCount() {
		return nonNullCount;
	}

	@Override
	public long getNullCount() {
		return nullCount;
	}

	public long getEmpytCount() {
		return empytCount;
	}

	public Integer getMinStringLength() {
		return minStringLength;
	}

	public Integer getMaxStringLength() {
		return maxStringLength;
	}

	public Double getMeanLength() {
		return meanLength;
	}

	@Override
	public String toString() {
		return "StringColumnSummary{" +
			"totalCount=" + getTotalCount() +
			", nonNullCount=" + nonNullCount +
			", nullCount=" + nullCount +
			", empytCount=" + empytCount +
			", minStringLength=" + minStringLength +
			", maxStringLength=" + maxStringLength +
			", meanLength=" + meanLength +
			'}';
	}
}
