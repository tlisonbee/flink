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

package org.apache.flink.test.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.summarize.NumericColumnSummary;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;

@RunWith(Parameterized.class)
public class DataSetUtilsITCase extends MultipleProgramsTestBase {

	public DataSetUtilsITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Test
	public void testZipWithIndex() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		long expectedSize = 100L;
		DataSet<Long> numbers = env.generateSequence(0, expectedSize - 1);

		List<Tuple2<Long, Long>> result = Lists.newArrayList(DataSetUtils.zipWithIndex(numbers).collect());

		Assert.assertEquals(expectedSize, result.size());
		// sort result by created index
		Collections.sort(result, new Comparator<Tuple2<Long, Long>>() {
			@Override
			public int compare(Tuple2<Long, Long> o1, Tuple2<Long, Long> o2) {
				return o1.f0.compareTo(o2.f0);
			}
		});
		// test if index is consecutive
		for (int i = 0; i < expectedSize; i++) {
			Assert.assertEquals(i, result.get(i).f0.longValue());
		}
	}

	@Test
	public void testZipWithUniqueId() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		long expectedSize = 100L;
		DataSet<Long> numbers = env.generateSequence(1L, expectedSize);

		DataSet<Long> ids = DataSetUtils.zipWithUniqueId(numbers).map(new MapFunction<Tuple2<Long,Long>, Long>() {
			@Override
			public Long map(Tuple2<Long, Long> value) throws Exception {
				return value.f0;
			}
		});

		Set<Long> result = Sets.newHashSet(ids.collect());

		Assert.assertEquals(expectedSize, result.size());
	}

	@Test
	public void testIntegerDataSetChecksumHashCode() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Integer> ds = CollectionDataSets.getIntegerDataSet(env);

		Utils.ChecksumHashCode checksum = DataSetUtils.checksumHashCode(ds);
		Assert.assertEquals(checksum.getCount(), 15);
		Assert.assertEquals(checksum.getChecksum(), 55);
	}

	@Test
	public void testSummarize() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		List<Tuple2<Integer, Double>> data = new ArrayList<>();
		data.add(new Tuple2<>(1, 1.012376));
		data.add(new Tuple2<>(2, 2.003453));
		data.add(new Tuple2<>(10, 75.00005));
		data.add(new Tuple2<>(4, 79.5));
		data.add(new Tuple2<>(5, 10.0000001));
		data.add(new Tuple2<>(6, 0.0000000000023));
		data.add(new Tuple2<>(7, 1000.000000000001));
		data.add(new Tuple2<>(8, 9000.00000000000006));

		Collections.shuffle(data);

		DataSet ds = env.fromCollection(data);
		Tuple results = DataSetUtils.summarize(ds);

		Assert.assertEquals(2, results.getArity());
		NumericColumnSummary<Integer> col0Summary = results.getField(0);
		Assert.assertEquals(8, col0Summary.getNonMissingCount());
		Assert.assertEquals(1, col0Summary.getMin().intValue());
		Assert.assertEquals(10, col0Summary.getMax().intValue());
		Assert.assertEquals(5.375, col0Summary.getMean().doubleValue(), 0.0);
		Assert.assertEquals(9.1249999999999998, col0Summary.getVariance().doubleValue(), 0.00000000001);
		Assert.assertEquals(3.0207614933986426, col0Summary.getStandardDeviation().doubleValue(), 0.0000000000001);

		NumericColumnSummary<Double> col1Summary = results.getField(1);
		Assert.assertEquals(8, col1Summary.getNonMissingCount());
		Assert.assertEquals(0.0000000000023, col1Summary.getMin().doubleValue(), 0.0);
		Assert.assertEquals(9000.00000000000006, col1Summary.getMax().doubleValue(), 0.000000000001);
		Assert.assertEquals(1270.9394848875002, col1Summary.getMean().doubleValue(), 0.000000000001);
		Assert.assertEquals(9869964.70032318, col1Summary.getVariance().doubleValue(), 0.00000001);
		Assert.assertEquals(3141.649996470514, col1Summary.getStandardDeviation().doubleValue(), 0.000000000001);
	}
}
