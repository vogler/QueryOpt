package parser;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import tinydb.Database;

public class Parser {

	// select (*|attribute(,attribute)*)
	// from relation binding(,relation binding)*
	// where binding.attribute=(binding.attribute|constant)
	// (and binding.attribute=(binding.attribute|constant))*
	public static void main(String[] args) throws java.io.IOException {
		
		//Database db = Database.open("../data/uni");
		Database db = Database.open("../../tpch/tpch");
		// select * from lineitem l, orders o, customer c where l.l_orderkey=o.o_orderkey and o.o_custkey=c.c_custkey and c.c_name='Customer#000014993'
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		
		System.out.println("Enter your query below. Type 'quit' to exit.");
		
		while(true){
			String line = br.readLine();
			if(line.equals("quit")) break;
			if(line.equals("r"))
				line = "select * from lineitem l, orders o, customer c where l.l_orderkey=o.o_orderkey and o.o_custkey=c.c_custkey and c.c_name='Customer#000014993'";
			
			try{
				Query query = Query.parse(line);
				PlanGenerator planGenerator = new PlanGenerator(db);
				QueryPlan queryPlan = planGenerator.parse(query);
				queryPlan.print();
				Executor.execute(queryPlan);
			}catch(Exception e){
				e.printStackTrace();
				continue;
			}
		}
		br.close();
	}
}
