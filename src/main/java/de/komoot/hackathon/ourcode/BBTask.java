package de.komoot.hackathon.ourcode;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vividsolutions.jts.geom.Geometry;

import de.komoot.hackathon.openstreetmap.GeometryModule;
import de.komoot.hackathon.openstreetmap.JsonGeometryEntity;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

@ConstantFields(fields = {})
@OutCardBounds(lowerBound = 0, upperBound = OutCardBounds.UNBOUNDED)
public class BBTask extends MapStub {
	
	public static class PactGeometry extends PactString{

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
		
		geometry.setValue(record.getField(0, PactString.class));
		
		outputRecord.setField(0, geometry);
		
		collector.collect(outputRecord);
	}

}
