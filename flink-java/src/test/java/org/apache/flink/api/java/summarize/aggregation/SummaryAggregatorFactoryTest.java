package org.apache.flink.api.java.summarize.aggregation;

import org.apache.flink.types.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;


public class SummaryAggregatorFactoryTest {

	@Test
	public void testCreate() throws Exception {
		// supported primitive types
		Assert.assertEquals(StringSummaryAggregator.class, SummaryAggregatorFactory.create(String.class).getClass());
		Assert.assertEquals(ShortSummaryAggregator.class, SummaryAggregatorFactory.create(Short.class).getClass());
		Assert.assertEquals(IntegerSummaryAggregator.class, SummaryAggregatorFactory.create(Integer.class).getClass());
		Assert.assertEquals(LongSummaryAggregator.class, SummaryAggregatorFactory.create(Long.class).getClass());
		Assert.assertEquals(FloatSummaryAggregator.class, SummaryAggregatorFactory.create(Float.class).getClass());
		Assert.assertEquals(DoubleSummaryAggregator.class, SummaryAggregatorFactory.create(Double.class).getClass());
		Assert.assertEquals(BooleanSummaryAggregator.class, SummaryAggregatorFactory.create(Boolean.class).getClass());

		// supported value types
		Assert.assertEquals(ValueSummaryAggregator.StringValueSummaryAggregator.class, SummaryAggregatorFactory.create(StringValue.class).getClass());
		Assert.assertEquals(ValueSummaryAggregator.ShortValueSummaryAggregator.class, SummaryAggregatorFactory.create(ShortValue.class).getClass());
		Assert.assertEquals(ValueSummaryAggregator.IntegerValueSummaryAggregator.class, SummaryAggregatorFactory.create(IntValue.class).getClass());
		Assert.assertEquals(ValueSummaryAggregator.LongValueSummaryAggregator.class, SummaryAggregatorFactory.create(LongValue.class).getClass());
		Assert.assertEquals(ValueSummaryAggregator.FloatValueSummaryAggregator.class, SummaryAggregatorFactory.create(FloatValue.class).getClass());
		Assert.assertEquals(ValueSummaryAggregator.DoubleValueSummaryAggregator.class, SummaryAggregatorFactory.create(DoubleValue.class).getClass());
		Assert.assertEquals(ValueSummaryAggregator.BooleanValueSummaryAggregator.class, SummaryAggregatorFactory.create(BooleanValue.class).getClass());

		// some not well supported types - these fallback to ObjectSummaryAggregator
		Assert.assertEquals(ObjectSummaryAggregator.class, SummaryAggregatorFactory.create(Object.class).getClass());
		Assert.assertEquals(ObjectSummaryAggregator.class, SummaryAggregatorFactory.create(List.class).getClass());
	}

}
