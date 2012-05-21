package parser.querygraph;

import parser.PairCondition;

public class Edge {

	private PairCondition predicate;
	private double selectivity;

	public Edge(PairCondition predicate, double selectivity){
		this.predicate = predicate;
		this.selectivity = selectivity;
	}
	
	@Override
	public String toString(){
		return predicate+" with estimated selectivity of "+selectivity;
	}
}
