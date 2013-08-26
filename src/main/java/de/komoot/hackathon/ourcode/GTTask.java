package de.komoot.hackathon.ourcode;

import de.komoot.hackathon.ourcode.BBTask.PactCoordinatesList;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

@ConstantFields(fields = {})
@OutCardBounds(lowerBound = 0, upperBound = OutCardBounds.UNBOUNDED)
public class GTTask extends MapStub{

	private final PactRecord outputRecord = new PactRecord();
	@Override
	public void map(PactRecord record, Collector<PactRecord> collector)
			throws Exception {
		//dummy solution so far
		outputRecord.setField(0, new PactInteger(0));
		outputRecord.setField(1, record.getField(0, PactString.class));
		outputRecord.setField(2, record.getField(1, PactCoordinatesList.class));
		outputRecord.setField(3, record.getField(2, PactCoordinatesList.class));
		
		collector.collect(record);
	}

}
