package parser.querygraph;

import java.util.Arrays;
import java.util.List;

import parser.PairCondition;
import parser.PairRelation;
import tinydb.Table;

public class Node {

	public PairRelation relation;
	public List<PairCondition> predicates;
	public int cardinality;
	public Table table;

	public Node(PairRelation relation, Table table, List<PairCondition> predicates, int cardinality){
		this.relation = relation;
		this.table = table;
		this.predicates = predicates;
		this.cardinality = cardinality;
	}
	
	@Override
	public String toString(){
		return "Relation: "+relation+", Pushed down predicates: "+Arrays.toString(predicates.toArray())+", Estimated cardinality: "+cardinality;
	}
}
