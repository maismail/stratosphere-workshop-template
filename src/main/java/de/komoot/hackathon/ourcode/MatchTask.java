package de.komoot.hackathon.ourcode;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;

public class MatchTask extends MatchStub{

	@Override
	public void match(PactRecord record1, PactRecord record2,
			Collector<PactRecord> collector) throws Exception {
		
	}
	

}
