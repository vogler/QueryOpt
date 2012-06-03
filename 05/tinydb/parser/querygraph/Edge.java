package parser.querygraph;

import java.util.Arrays;
import java.util.List;

import parser.PairCondition;
import parser.PlanGenerator;
import tinydb.Attribute;

public class Edge {

	public Node node_a;
	public Node node_b;
	public List<PairCondition> predicates;
//	public double selectivity;

	public Edge(Node node_a, Node node_b, List<PairCondition> predicates){
		this.node_a = node_a;
		this.node_b = node_b;
		this.predicates = predicates;
//		this.selectivity = 1;
	}
	
	@Override
	public String toString(){
		return Arrays.toString(predicates.toArray())+" with estimated selectivity of "+getSelectivity();
	}
	
	public double getSelectivity(){
		// estimate selectivity for join predicate
		double selectivity = 1;
		for(PairCondition cond : predicates){
			Attribute attr_a = PlanGenerator.getAttribute(node_a.table, cond.getAttributes().a);
			Attribute attr_b = PlanGenerator.getAttribute(node_b.table, cond.getAttributes().b);
			if(attr_a == null || attr_b == null) continue;
			if(attr_a.getKey() && attr_b.getKey()){ // both keys
				selectivity *= 1./Math.max(node_a.cardinality, node_b.cardinality);
			}else if(!attr_a.getKey() && !attr_b.getKey()){ // both not keys
				selectivity *= 1./Math.max(attr_a.getUniqueValues(), attr_b.getUniqueValues());
			}else{ // exactly one key
				selectivity *= 1./(attr_a.getKey() ? node_a.cardinality : node_b.cardinality);
			}
		}
		return selectivity;
	}
}
