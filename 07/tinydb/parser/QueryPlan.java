package parser;

import java.io.IOException;
import java.util.List;

import parser.querygraph.Edge;
import parser.querygraph.Node;
import tinydb.operator.Operator;
import tinydb.operator.Printer;


public class QueryPlan {
	

	private Operator select;
	private List<String> plan;
	private List<Node> nodes;
	private List<Edge> edges;

	public QueryPlan(Operator select, List<String> plan, List<Node> nodes, List<Edge> edges){
		this.select = select;
		this.plan = plan;
		this.nodes = nodes;
		this.edges = edges;
	}
	
	public void execute() throws IOException{
		System.out.println("-- Result:");
		Printer out = new Printer(select);
		out.open();
		while (out.next()){
			
		}
		out.close();
	}
	
	public void printJoinTree(){
		// TODO library for printing binary trees?
	}
	
	public void printExecutionPlan(){
		System.out.println("-- Execution plan:");
		for(String s : plan){
			System.out.println(s);
		}
		System.out.println();
	}

	private void printQueryGraph() {
		System.out.println("-- Query graph:");
		System.out.println("- Nodes:");
		for(Node node : nodes){
			System.out.println(node);
		}
		System.out.println("- Edges:");
		for(Edge edge : edges){
			System.out.println(edge);
		}
		System.out.println();
	}
	
	public void print(){
		printJoinTree();
		printExecutionPlan();
		printQueryGraph();
	}

}
