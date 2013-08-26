package de.komoot.hackathon.ourcode;

import de.komoot.hackathon.ourcode.FReduce.PactStringList;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class KomootStratosphere implements PlanAssembler, PlanAssemblerDescription{
	
	@Override
	public String getDescription() {
		return "Komoot Stratosphere task";
	}

	@Override
	public Plan getPlan(String... args) {
		int noSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String dataInput1 = (args.length > 1 ? args[1] : "");
		String dataInput2 = (args.length > 2 ? args[2] : "");
		
		String output = (args.length > 3 ? args[3] : "");

		FileDataSource source1 = new FileDataSource(TextInputFormat.class,
				dataInput1, "nodes data");
		
		FileDataSource source2 = new FileDataSource(TextInputFormat.class,
				dataInput2, "areas data");
		
		source1.setParameter(TextInputFormat.CHARSET_NAME, "ASCII");
		source2.setParameter(TextInputFormat.CHARSET_NAME, "ASCII");
		
		
		MapContract bbmapper1 = MapContract.builder(BBTask.class).input(source1).name("BBTask for nodes").build();
		MapContract bbmapper2 = MapContract.builder(BBTask.class).input(source2).name("BBTask for areas").build();
		
	
		MapContract gtmapper1 = MapContract.builder(GTTask.class).input(bbmapper1).name("GTTask for nodes").build();
		MapContract gtmapper2 = MapContract.builder(GTTask.class).input(bbmapper2).name("GTTask for areas").build();
		
		MatchContract matcher = MatchContract.builder(MatchTask.class, PactInteger.class, 0, 0).input1(gtmapper1).input2(gtmapper2).name("Matcher").build();
		
		ReduceContract reducer = ReduceContract.builder(FReduce.class).input(matcher).keyField(PactString.class, 0).name("Reducer").build();
		
		FileDataSink out = new FileDataSink(RecordOutputFormat.class, output,
				reducer, "Result");
		
		RecordOutputFormat.configureRecordFormat(out).recordDelimiter('\n')
		.fieldDelimiter(',').lenient(true).field(PactString.class, 0)
		.field(PactStringList.class, 1);

		Plan plan = new Plan(out, "BB");
		plan.setDefaultParallelism(noSubTasks);
		return plan;
	}

}
