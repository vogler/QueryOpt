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
//		Database db = Database.open("../data/uni");
		Database db = Database.open("../../../03/tpch/tpch");
		Executor executor = new Executor(db);
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		
		System.out.println("Enter your query below. Type 'quit' to exit.");
		
		while(true){
			String line = br.readLine();
			if(line.equals("quit")) break;
			
			try{
				Query query = Query.parse(line);
				executor.execute(query);
			}catch(Exception e){
				continue;
			}
		}
		br.close();
	}
}
