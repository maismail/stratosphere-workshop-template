package de.komoot.hackathon.ourcode;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class MedianSearch extends CoGroupStub {

	private final List<Double> values = new ArrayList<Double>();
	private final Random random = new Random();
	private String orientation = "HORIÍZONTAL";
	private int cellId;
	private boolean first;
	private final static Log LOG = LogFactory.getLog(SplitByMedian.class);
	
	@Override
	public void open(Configuration parameters) throws Exception {
		// TODO Auto-generated method stub
		super.open(parameters);
		orientation = parameters.getString("orientation", "HORIZONTAL");
		values.clear();
		cellId = -1;
		first = true;
	}
	
	double kmedian(int k, List<Double> numbers) {
		int index = random.nextInt(numbers.size());
		double borderValue = numbers.get(index);
		ArrayList<Double> smallers = new ArrayList<Double>();
		ArrayList<Double> bigers = new ArrayList<Double>();
		ArrayList<Double> same = new ArrayList<Double>();
		for (Double num : numbers) {
			if (num == borderValue) {
				same.add(num);
			} else if (num < borderValue) {
				smallers.add(num);
			} else {
				bigers.add(num);
			}
		}

		if (k > smallers.size()) {
			if (smallers.size() + same.size() >= k) {
				return borderValue;
			}
			return kmedian(k - smallers.size() - same.size(), bigers);
		} else {
			return kmedian(k, smallers);
		}
	}



	
	
	private void getValues(Iterator<PactRecord> records) {
		while (records.hasNext()) {
			PactRecord pactRecord = (PactRecord) records.next();
			if (first) {
				cellId = pactRecord.getField(0, PactInteger.class).getValue();
				first = false;
			}
			if (orientation.equals("HORIZONTAL")) {
				values.add(pactRecord.getField(1, PactDouble.class).getValue());
			} else {
				values.add(pactRecord.getField(2, PactDouble.class).getValue());
			}
		}
	}
	
	@Override
	public void coGroup(Iterator<PactRecord> records1,
			Iterator<PactRecord> records2, Collector<PactRecord> out) {
		first = true;
		getValues(records1);
		getValues(records2);
		PactRecord outRecord = new PactRecord(2);
		outRecord.setField(0, new PactInteger(cellId));
		outRecord.setField(1, new PactDouble(kmedian(values.size() / 2, values)));
		out.collect(outRecord);
		LOG.info("outrecord: " + cellId);
					// TODO Auto-generated method stub
	}

}
