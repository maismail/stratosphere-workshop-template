package de.komoot.hackathon.ourcode;

import com.vividsolutions.jts.geom.Geometry;

import de.komoot.hackathon.openstreetmap.JsonGeometryEntity;
import de.komoot.hackathon.ourcode.BBTask.PactGeometry;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class MatchTask extends MatchStub{

	private final PactRecord outputRecord = new PactRecord();
	private final PactString id = new PactString();
	
	@Override
	public void match(PactRecord record1, PactRecord record2,
			Collector<PactRecord> collector) throws Exception {
		
		//we should check for the cell first
		JsonGeometryEntity<Geometry> geo1 = record1.getField(1, PactGeometry.class).getGeometry();
		JsonGeometryEntity<Geometry> geo2 = record2.getField(1, PactGeometry.class).getGeometry();
		if(geo1.getGeometry().intersects(geo2.getGeometry())){
			id.setValue(geo1.getId());
			outputRecord.setField(0, id);
			id.setValue(geo2.getId());
			outputRecord.setField(1, id);
			collector.collect(outputRecord);
		}
	}
}
