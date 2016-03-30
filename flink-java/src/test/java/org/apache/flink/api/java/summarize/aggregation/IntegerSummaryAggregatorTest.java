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

package org.apache.flink.api.java.summarize.aggregation;

import org.apache.flink.api.java.summarize.NumericColumnSummary;
import org.junit.Assert;
import org.junit.Test;


public class IntegerSummaryAggregatorTest {

	@Test
	public void testIsNan() throws Exception {
		IntegerSummaryAggregator ag = new IntegerSummaryAggregator();
		// always false for Integer
		Assert.assertFalse(ag.isNan(-1));
		Assert.assertFalse(ag.isNan(0));
		Assert.assertFalse(ag.isNan(23));
		Assert.assertFalse(ag.isNan(Integer.MAX_VALUE));
		Assert.assertFalse(ag.isNan(Integer.MIN_VALUE));
		Assert.assertFalse(ag.isNan(null));
	}

	@Test
	public void testIsInfinite() throws Exception {
		IntegerSummaryAggregator ag = new IntegerSummaryAggregator();
		// always false for Integer
		Assert.assertFalse(ag.isInfinite(-1));
		Assert.assertFalse(ag.isInfinite(0));
		Assert.assertFalse(ag.isInfinite(23));
		Assert.assertFalse(ag.isInfinite(Integer.MAX_VALUE));
		Assert.assertFalse(ag.isInfinite(Integer.MIN_VALUE));
		Assert.assertFalse(ag.isInfinite(null));
	}

	@Test
	public void testMean() throws Exception {
		Assert.assertEquals(50.0, mean(0, 100), 0.0);
		Assert.assertEquals(33.333333, mean(0, 0, 100), 0.00001);
		Assert.assertEquals(50.0, mean(0, 0, 100, 100), 0.0);
		Assert.assertEquals(50.0, mean(0, 100, null), 0.0);
		Assert.assertNull(mean());
	}

	@Test
	public void testSum() throws Exception {
		Assert.assertEquals(100, sum(0, 100).intValue());
		Assert.assertEquals(15, sum(1, 2, 3, 4, 5).intValue());
		Assert.assertEquals(0, sum(-100, 0, 100, null).intValue());
		Assert.assertEquals(90, sum(-10, 100, null).intValue());
		Assert.assertNull(sum());
	}

	private static Integer sum(Integer... values) {
		return aggregate(values).getSum();
	}

	private static Double mean(Integer... values) {
		return aggregate(values).getMean();
	}

	private static NumericColumnSummary<Integer> aggregate(Integer... values) {
		IntegerSummaryAggregator aggregator = new IntegerSummaryAggregator();
		for(Integer value: values) {
			aggregator.aggregate(value);
		}
		return aggregator.result();
	}

}
