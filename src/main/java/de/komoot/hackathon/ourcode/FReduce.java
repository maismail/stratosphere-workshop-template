package de.komoot.hackathon.ourcode;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactList;
import eu.stratosphere.pact.common.type.base.PactString;

public class FReduce extends ReduceStub {
	
	public static class PactStringList extends PactList<PactString>{
		public PactStringList(){
			super();
		}
		
		public PactStringList(PactString... strs){
			for(PactString str : strs){
				add(str);
			}
		}
	}
	
	private final PactRecord outputRecord = new PactRecord();
	@Override
	public void reduce(Iterator<PactRecord> records,
			Collector<PactRecord> out) throws Exception {
		PactString key = null;
		PactStringList datalist = new PactStringList();
		while (records.hasNext()) {
			PactRecord element = records.next();
			if(key == null)
				key = element.getField(0, PactString.class);
			datalist.add(element.getField(1, PactString.class));
		}
		outputRecord.setField(0, key);
		outputRecord.setField(1, datalist);
		out.collect(outputRecord);
	}

}
