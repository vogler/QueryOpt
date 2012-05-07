import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import tinydb.Database;
import tinydb.Register;
import tinydb.Table;
import tinydb.operator.CrossProduct;
import tinydb.operator.Operator;
import tinydb.operator.Printer;
import tinydb.operator.Projection;
import tinydb.operator.Selection;
import tinydb.operator.Tablescan;

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
			p = Pattern.compile("\\w+\\.\\w+=(\\w+\\.\\w+|\\w+)(and \\w+\\.\\w+=(\\w+\\.\\w+|\\w+))*");
			if(!p.matcher(joincond).matches()){
				System.err.println("Join conditions don't match the pattern: "+p.toString());
				System.err.println("Your input: "+joincond);
				continue;
			}
			
			// store the query structure (to file?)
			BufferedWriter bw = new BufferedWriter(new FileWriter("query.txt"));
			bw.write("relations: "+relations);
			bw.newLine();
			bw.write("selections: "+selections);
			bw.newLine();
			bw.write("joinconditions: "+joincond);
			bw.close();
			
			
			// handle relations
			Map<String, Table> h_rel = new HashMap<String, Table>();
			Map<String, Tablescan> h_scans = new HashMap<String, Tablescan>();
			String[] a_rel = relations.split(",");
			// accumulator for cross products
			Operator cp = null;
			for(String s : a_rel){
				String[] binding = s.split(" ");
				String tablename = binding[0];
				String bindingname = binding[1];
				Table table = db.getTable(tablename);
				if(table == null){
					System.err.println("Table "+tablename+" doesn't exist");
					continue loop;
				}
				h_rel.put(bindingname, table);
				Tablescan tablescan = new Tablescan(table);
				if(!h_scans.containsKey(bindingname)){
					h_scans.put(bindingname, tablescan);
				}
				if(cp == null){
					cp = tablescan;
				}else{
					cp = new CrossProduct(cp, tablescan);
				}
			}
			
			// handle join conditions
			List<Pair> a_cond = new ArrayList<Pair>();
			for(String cond : joincond.split(" and ")){
				String expr[] = cond.split("=");
				String sa = expr[0];
				String sb = expr[1];
				// left side of condition
				// TODO extract method
				String[] a_binding = sa.split("\\.");
				Table t_a = h_rel.get(a_binding[0]);
				if(t_a == null){
					System.err.println("There is no table for binding "+a_binding[0]+" in condition "+cond);
					continue loop;
				}
				int a_attr = t_a.findAttribute(a_binding[1]);
				if(a_attr == -1){
					System.err.println("There is no attribute "+a_binding[1]+" for binding "+a_binding[0]+" in condition "+cond);
					continue loop;
				}
				Register a = h_scans.get(a_binding[0]).getOutput()[a_attr];
				// right side of condition
				Register b;
				if(sb.contains(".")){
					String[] b_binding = sb.split("\\.");
					Table t_b = h_rel.get(b_binding[0]);
					if(t_b == null){
						System.err.println("There is no table for binding "+b_binding[0]+" in condition "+cond);
						continue loop;
					}
					int b_attr = t_b.findAttribute(b_binding[1]);
					if(b_attr == -1){
						System.err.println("There is no attribute "+b_binding[1]+" for binding "+b_binding[0]+" in condition "+cond);
						continue loop;
					}
					b = h_scans.get(b_binding[0]).getOutput()[b_attr];
				}else{
					// constant
					try{
						int i = Integer.parseInt(sb);
						b = new Register(i);
					}catch(NumberFormatException e){
						b = new Register(sb);
					}
				}
				// add condition pair to list
				a_cond.add(new Pair(a, b));
			}
			// do selections with conditions from list on cross product
			Operator select = cp;
			for(Pair cond : a_cond){
				select = new Selection(select, cond.a, cond.b);
			}
			
			// handle projections
			List<Register> a_proj = new ArrayList<Register>();
			if(!selections.equals("*")){
				String[] s = selections.split(",");
				// check if attributes exist. problem: on which table? binding missing in definition?
				// -> go through all tables for every attribute
				attrloop:
				for(String attr : s){
					for(Entry<String, Table> e : h_rel.entrySet()){
						int i = e.getValue().findAttribute(attr);
						if(i != -1){
							Register r = h_scans.get(e.getKey()).getOutput()[i];
							a_proj.add(r);
							continue attrloop;
						}
					}
				}
				// do projection
				Projection project = new Projection(select, a_proj.toArray(new Register[0]));
				select = project;
			}
			
			Printer out = new Printer(select);
			out.open();
			while (out.next()){
				
			}
			out.close();
		}
		
		br.close();
	}
	
	static private class Pair {
		public Register a;
		public Register b;
		public Pair(Register a, Register b){
			this.a = a;
			this.b = b;
		}
	}
}
