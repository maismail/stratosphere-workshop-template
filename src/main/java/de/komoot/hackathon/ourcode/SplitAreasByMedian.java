package de.komoot.hackathon.ourcode;

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class SplitAreasByMedian extends CoGroupStub {
	
	private String orientation = "HORIZONTAL";
	private boolean isFirst = true;
	private int id = -1;
	private final static Log LOG = LogFactory.getLog(SplitNodesByMedian.class);
	
	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		orientation = parameters.getString("orientation", "HORIZONTAL");
	}
	
	@Override
	public void coGroup(Iterator<PactRecord> records1,
			Iterator<PactRecord> records2, Collector<PactRecord> out) {
		if (!records2.hasNext()) {
			return;
		}
//		LOG.info("records1.hasNext() " + records1.hasNext());
//		LOG.info("records2.hasNext() " + records2.hasNext());
//		LOG.info("records2 id " + records2.next().getField(0, PactInteger.class).getValue());
		double border = records1.next().getField(1, PactDouble.class).getValue();
		while (records2.hasNext()) {
			PactRecord pactRecord = (PactRecord) records2.next();
			if (isFirst) {
				id = pactRecord.getField(0, PactInteger.class).getValue();
			}
			if (id < 0) {
				out.collect(pactRecord);
				break;
			}
			boolean isLeftToLine = false;
			boolean isRightToLine = false;
			for (int i = orientation.equals("HORIZONTAL") ? 1 : 2; i < 9; i = i + 2) {
				double value = pactRecord.getField(i, PactDouble.class).getValue();
				if (value > border) {
					isRightToLine = true;
				} else if (value < border) {
					isLeftToLine = true;
				}
			}
			int newId;
			if (isLeftToLine && isRightToLine) {
				newId = -id;
			}
			if (isLeftToLine) {
				newId = 2 * id;
			} else {
				newId = 2 * id + 1;
			}
			
			
			pactRecord.setField(0, new PactInteger(newId));
			out.collect(pactRecord);
		}
	}

}
