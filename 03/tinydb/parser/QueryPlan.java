package parser;

import java.io.IOException;
import java.util.List;

import tinydb.operator.Operator;
import tinydb.operator.Printer;


public class QueryPlan {
	

	private Operator select;
	private List<String> plan;

	public QueryPlan(Operator select, List<String> plan){
		this.select = select;
		this.plan = plan;
	}
	
	public void execute() throws IOException{
		Printer out = new Printer(select);
		out.open();
		while (out.next()){
			
		}
		out.close();
	}
	
	public void printJoinTree(){
		// TODO library for printing b-trees?
	}
	
	public void printExecutionPlan(){
		System.out.println("Execution plan:");
		for(String s : plan){
			System.out.println(s);
		}
		System.out.println();
	}
	
	public void print(){
		printJoinTree();
		printExecutionPlan();
	}

}
