package de.komoot.hackathon.ourcode;

import java.util.HashSet;
import java.util.Set;

import com.vividsolutions.jts.geom.Envelope;

import de.komoot.hackathon.ourcode.BBTask.PactGeometry;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

@ConstantFields(fields = {})
@OutCardBounds(lowerBound = 0, upperBound = OutCardBounds.UNBOUNDED)
public class GTTask extends MapStub{

	private class Tuple{
		private int gridX;
		private int gridY;
	}
	
	private final PactRecord outputRecord = new PactRecord();
	private final double GRID_X = 8;
	private final double GRID_Y = 53;
	private final int CELL_PER_DEGREE = 90;
	
	@Override
	public void map(PactRecord record, Collector<PactRecord> collector)
			throws Exception {
		Set<Integer> gridCells = getLocationInGrid(record.getField(0, PactGeometry.class).getGeometry().getGeometry().getEnvelopeInternal());
		
		for(Integer gridCell : gridCells){
			outputRecord.setField(0, new PactInteger(gridCell));
			outputRecord.setField(1, record.getField(0, PactGeometry.class));
			collector.collect(outputRecord);
		}
	}
	
	
	private Set<Integer> getLocationInGrid(Envelope envelope){
		Tuple min = new Tuple();
		min.gridX = (int) ((envelope.getMinX() - GRID_X) * CELL_PER_DEGREE);
		min.gridY = (int) ((envelope.getMinY() - GRID_Y) * CELL_PER_DEGREE);

		Tuple max = new Tuple();
		max.gridX = (int) ((envelope.getMaxX() - GRID_X) * CELL_PER_DEGREE);
		max.gridY = (int) ((envelope.getMaxY() - GRID_Y) * CELL_PER_DEGREE);
		Set<Integer> gridCells = new HashSet<Integer>();
		for(int x = min.gridX; x<= max.gridX; x++){
			for(int y = min.gridY; y<=max.gridY; y++){
				int gridCell = x + (y * CELL_PER_DEGREE);
				gridCells.add(gridCell);
			}
		}
		return gridCells;
	}
}
