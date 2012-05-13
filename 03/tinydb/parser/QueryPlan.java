package parser;

import java.io.IOException;

import tinydb.operator.Operator;
import tinydb.operator.Printer;


public class QueryPlan {
	

	private Operator select;

	public QueryPlan(Operator select){
		this.select = select;
	}
	
	public void execute() throws IOException{
		Printer out = new Printer(select);
		out.open();
		while (out.next()){
			
		}
		out.close();
	}
	
	public void printJoinTree(){
		
	}
	
	public void printExecutionPlan(){
		
	}
	
	public void print(){
		printJoinTree();
		printExecutionPlan();
	}

}
