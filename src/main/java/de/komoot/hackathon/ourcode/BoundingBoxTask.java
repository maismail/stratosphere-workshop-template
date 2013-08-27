package de.komoot.hackathon.ourcode;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

import de.komoot.hackathon.openstreetmap.GeometryModule;
import de.komoot.hackathon.openstreetmap.JsonGeometryEntity;
import de.komoot.hackathon.ourcode.BBTask.PactGeometry;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class BoundingBoxTask extends MapStub {

	
	public static class PactGeometry extends PactString {

		public static ObjectMapper mapper = new ObjectMapper();
		
		public PactGeometry() {
			super();
			mapper.registerModule(new GeometryModule());
			mapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
		}
		
		public JsonGeometryEntity<Geometry> getGeometry() throws IOException{
			return mapper.readValue(getValue(), JsonGeometryEntity.class);
		}
		
	}
	
	private final PactRecord outputRecord = new PactRecord();
	private final PactGeometry geometry = new PactGeometry();	
		
	@Override
	public void map(PactRecord record, Collector<PactRecord> collector)
			throws Exception {
		outputRecord.setField(0, new PactInteger(1));
		geometry.setValue(record.getField(0, PactGeometry.class));
		outputRecord.setField(3, geometry);
		Coordinate[] coordinates = geometry.getGeometry().getGeometry().getEnvelope().getCoordinates();
		for (int i = 0; i < coordinates.length; ++i) {
			outputRecord.setField(1, new PactDouble(coordinates[i].x));
			outputRecord.setField(2, new PactDouble(coordinates[i].y));
			collector.collect(outputRecord);
		}
	}
}
