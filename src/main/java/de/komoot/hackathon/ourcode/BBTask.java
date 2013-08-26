package de.komoot.hackathon.ourcode;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

import de.komoot.hackathon.openstreetmap.GeometryModule;
import de.komoot.hackathon.openstreetmap.JsonGeometryEntity;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactList;
import eu.stratosphere.pact.common.type.base.PactPair;
import eu.stratosphere.pact.common.type.base.PactString;

@ConstantFields(fields = {})
@OutCardBounds(lowerBound = 0, upperBound = OutCardBounds.UNBOUNDED)
public class BBTask extends MapStub {

	public static class DoubleDoublePactPair extends PactPair<PactDouble, PactDouble> {
		public DoubleDoublePactPair() {}
		
		public DoubleDoublePactPair(double first, double second) {
			super(new PactDouble(first), new PactDouble(second));
		}
		
		public DoubleDoublePactPair(PactDouble first, PactDouble second) {
			super(first, second);
		}
		
	}
	
	public static class PactCoordinatesList extends PactList<DoubleDoublePactPair> {
		public PactCoordinatesList() {}
		
		public PactCoordinatesList(DoubleDoublePactPair... coordinates) {
			for (DoubleDoublePactPair coordinate : coordinates) {
				add(coordinate);
			}
		}
		
		public void addPair(double first, double second) {
			add(new DoubleDoublePactPair(first, second));
		}
	}
	
	private final ObjectMapper mapper;
	private final PactRecord outputRecord = new PactRecord();
	private final PactString id = new PactString();
	private final PactCoordinatesList coordinates = new PactCoordinatesList();
	private final PactCoordinatesList envelope = new PactCoordinatesList();

	
	public BBTask() {
		mapper = new ObjectMapper();
		mapper.registerModule(new GeometryModule());
		mapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
	}
			
		
	@Override
	public void map(PactRecord record, Collector<PactRecord> collector)
			throws Exception {
		PactString line = record.getField(0, PactString.class);
		JsonGeometryEntity<Geometry> elem =  
				mapper.readValue(line.getValue(), JsonGeometryEntity.class);
		
		id.setValue(elem.getId());
		coordinates.clear();
		for(Coordinate cr : elem.getGeometry().getCoordinates()) {
			coordinates.addPair(cr.x, cr.y);
		}
		
		outputRecord.setField(0, id);
		outputRecord.setField(1, coordinates);
		
		envelope.clear();
		for(Coordinate cr : elem.getGeometry().getEnvelope().getCoordinates()) {
			envelope.addPair(cr.x, cr.y);
		}
		outputRecord.setField(2, envelope);
		
		collector.collect(outputRecord);
	}

}
