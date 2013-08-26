package de.komoot.hackathon.ourcode;

import de.komoot.hackathon.ourcode.BBTask.PactCoordinatesList;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactString;

public class KomootStratosphere implements PlanAssembler, PlanAssemblerDescription{
	
	@Override
	public String getDescription() {
		return "Komoot Stratosphere task";
	}

	@Override
	public Plan getPlan(String... args) {
		int noSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String dataInput = (args.length > 1 ? args[1] : "");
		String output = (args.length > 2 ? args[2] : "");

		FileDataSource source = new FileDataSource(TextInputFormat.class,
				dataInput, "Input Lines");
		source.setParameter(TextInputFormat.CHARSET_NAME, "ASCII"); // comment
																	// out this
																	// line for
																	// UTF-8
																	// inputs
		
		MapContract mapper = MapContract.builder(BBTask.class).input(source).name("BBTask").build();
		
			
		FileDataSink out = new FileDataSink(RecordOutputFormat.class, output,
		mapper, "Result");
		
		RecordOutputFormat.configureRecordFormat(out).recordDelimiter('\n')
		.fieldDelimiter(',').lenient(true).field(PactString.class, 0)
		.field(PactCoordinatesList.class, 1).field(PactCoordinatesList.class, 2);

		Plan plan = new Plan(out, "BB");
		plan.setDefaultParallelism(noSubTasks);
		return plan;
	}

}
