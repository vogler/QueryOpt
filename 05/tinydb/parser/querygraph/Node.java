package parser.querygraph;

import java.util.Arrays;
import java.util.List;

import parser.PairCondition;
import parser.PairRelation;

public class Node {

	private PairRelation relation;
	private List<PairCondition> predicates;
	private int cardinality;

	public Node(PairRelation relation, List<PairCondition> predicates, int cardinality){
		this.relation = relation;
		this.predicates = predicates;
		this.cardinality = cardinality;
	}
	
	@Override
	public String toString(){
		return "Relation: "+relation+", Pushed down predicates: "+Arrays.toString(predicates.toArray())+", Estimated cardinality: "+cardinality;
	}
}
