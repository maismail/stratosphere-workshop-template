package de.komoot.hackathon.ourcode;

import de.komoot.hackathon.ourcode.FReduce.PactStringList;
import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class TreeBuildingPlan implements PlanAssembler,
		PlanAssemblerDescription {

	@Override
	public String getDescription() {
		return "Komoot problem solving with an adaptive tree";
	}

	@Override
	public Plan getPlan(String... args) {
		int noSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String dataInput1 = (args.length > 1 ? args[1] : "");
		String dataInput2 = (args.length > 2 ? args[2] : "");
		String output = (args.length > 3 ? args[3] : "");
		int numberOfIterations = (args.length > 4 ? Integer.valueOf(args[4]) : 10);
		
//		CoGroupContract[] medianSearches = new CoGroupContract[numberOfIterations];
//		CoGroupContract[] areaSplits = new CoGroupContract[numberOfIterations];
//		 CoGroupContract[] nodeSplits = new CoGroupContract[numberOfIterations];
		
		

		FileDataSource nodeSource = new FileDataSource(TextInputFormat.class,
				dataInput1, "nodes data");
		
		FileDataSource areaSource = new FileDataSource(TextInputFormat.class,
				dataInput2, "areas data");
		
		nodeSource.setParameter(TextInputFormat.CHARSET_NAME, "ASCII");
		areaSource.setParameter(TextInputFormat.CHARSET_NAME, "ASCII");
		
		MapContract nodeMapper = MapContract.builder(BoundingBoxTask.class).input(nodeSource).name("BBTask for nodes").build();
		MapContract areaMapper = MapContract.builder(BoundingBoxTask.class).input(areaSource).name("BBTask for areas").build();
		
		CoGroupContract medianSearch = CoGroupContract.builder(MedianSearch.class, PactInteger.class, 0, 0).input1(nodeMapper).input2(areaMapper).name("median 0").build();
		medianSearch.setParameter("orientation",  "HORIZONTAL");
		CoGroupContract nodeSplit = CoGroupContract.builder(SplitByMedian.class, PactInteger.class, 0, 0).input1(medianSearch).input2(nodeMapper).name("node split 0").build();
		nodeSplit.setParameter("orientation",  "HORIZONTAL");
		CoGroupContract areaSplit = CoGroupContract.builder(SplitByMedian.class, PactInteger.class, 0, 0).input1(medianSearch).input2(areaMapper).name("area split 0").build();
		areaSplit.setParameter("orientation",  "HORIZONTAL");
		CoGroupContract previousNodeSplit = nodeSplit;
		CoGroupContract previousAreaSplit = areaSplit;
		String orientation = "VERTICAL";
		for (int i = 1; i < numberOfIterations; ++i) {
			orientation = 1 == i % 2 ? "VERTICAL" : "HORIZONTAL"; 
			medianSearch = CoGroupContract.builder(MedianSearch.class, PactInteger.class, 0, 0).input1(previousNodeSplit).input2(previousAreaSplit).name("median " + i).build();
			medianSearch.setParameter("orientation",  orientation);
			nodeSplit = CoGroupContract.builder(SplitByMedian.class, PactInteger.class, 0, 0).input1(medianSearch).input2(previousNodeSplit).name("node split " + i).build();
			nodeSplit.setParameter("orientation",  orientation);
			areaSplit = CoGroupContract.builder(SplitByMedian.class, PactInteger.class, 0, 0).input1(medianSearch).input2(previousAreaSplit).name("area split " + i).build();
			areaSplit.setParameter("orientation",  orientation);
			previousNodeSplit = nodeSplit;
			previousAreaSplit = areaSplit;	
		}
		FileDataSink out = new FileDataSink(RecordOutputFormat.class, output,
				previousAreaSplit, "Result");
		
		RecordOutputFormat.configureRecordFormat(out).recordDelimiter('\n')
		.fieldDelimiter(',').lenient(true).field(PactInteger.class, 0);

		Plan plan = new Plan(out, "BB");
		plan.setDefaultParallelism(noSubTasks);
		return plan;
		
	}

}
