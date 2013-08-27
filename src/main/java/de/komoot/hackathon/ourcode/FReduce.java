package de.komoot.hackathon.ourcode;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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

		@Override
		public String toString() {
			return super.toString().replace("[", "").replace("]", "").replace(" ", "");
		}
		
	}
	
	private final PactRecord outputRecord = new PactRecord();
	
	@Override
	public void reduce(Iterator<PactRecord> records,
			Collector<PactRecord> out) throws Exception {
		PactString key = null;
		Set<String> datasets = new HashSet<String>();
		while (records.hasNext()) {
			PactRecord element = records.next();
			if(key == null){
				key = element.getField(0, PactString.class);
			}
			datasets.add(element.getField(1, PactString.class).getValue());
		}
		PactStringList datalist = new PactStringList();
		for(String str : datasets){
			datalist.add(new PactString(str));
		}
		outputRecord.setField(0, key);
		outputRecord.setField(1, new PactString(""));
		outputRecord.setField(2, datalist);
		out.collect(outputRecord);
	}

}
