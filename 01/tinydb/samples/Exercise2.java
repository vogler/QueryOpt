import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import tinydb.Database;
import tinydb.Table;

public class Exercise2 {

	// select (*|attribute(,attribute)*)
	// from relation binding(,relation binding)*
	// where binding.attribute=(binding.attribute|constant)
	// (and binding.attribute=(binding.attribute|constant))*
	public static void main(String[] args) throws java.io.IOException {
		Database db = Database.open("../data/uni");
		System.out.println("Enter your query below. Type 'quit' to exit.");
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		loop:
		while(true){
			String q = br.readLine();
			if(q.equals("quit")) break;
			Pattern p = Pattern.compile("select (.+) from (.+) where (.+)");
			Matcher m = p.matcher(q);
			if(!m.matches()){
				System.err.println("Query doesn't match the pattern: "+p.toString());
				continue;
			}
			String selections = m.group(1);
			String relations = m.group(2);
			String joincond = m.group(3);
			// (*|attribute(,attribute)*)
			p = Pattern.compile("\\*|\\w+(,\\w+)*");
			if(!p.matcher(selections).matches()){
				System.err.println("Selections don't match the pattern: "+p.toString());
				System.err.println("Your input: "+selections);
				continue;
			}
			// relation binding(,relation binding)*
			p = Pattern.compile("\\w+ \\w+(,\\w+ \\w+)*");
			if(!p.matcher(relations).matches()){
				System.err.println("Relations don't match the pattern: "+p.toString());
				System.err.println("Your input: "+relations);
				continue;
			}
			// binding.attribute=(binding.attribute|constant)
			// (and binding.attribute=(binding.attribute|constant))*
			p = Pattern.compile("\\w+\\.\\w+=\\w+(and \\w+\\.\\w+=\\w+)*");
			if(!p.matcher(joincond).matches()){
				System.err.println("Join conditions don't match the pattern: "+p.toString());
				System.err.println("Your input: "+joincond);
				continue;
			}
			
			// TODO save values in file
			
			// handle relations
			Map<String, Table> h_rel = new HashMap<String, Table>();
			String[] a_rel = relations.split(",");
			for(String s : a_rel){
				String[] binding = s.split(" ");
				System.out.println("trying to open "+binding[0]);
				Table table = db.getTable(binding[0]);
				if(table == null){ // TODO java.io.IOException: Stream closed
					System.err.println("Table "+binding[0]+" doesn't exist");
					continue loop;
				}
				h_rel.put(binding[1], table);
			}
			
			
			br.close();
		}
	}
}
