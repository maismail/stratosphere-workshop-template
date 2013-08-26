package de.komoot.hackathon.ourcode;

import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;

public class KomootStratosphere implements PlanAssembler, PlanAssemblerDescription{
	
	@Override
	public String getDescription() {
		return "Komoot Stratosphere task";
	}

	@Override
	public Plan getPlan(String... arg0) {
		// TODO Auto-generated method stub
		return null;
	}

}
