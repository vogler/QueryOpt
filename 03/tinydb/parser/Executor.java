package parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import tinydb.Database;
import tinydb.Register;
import tinydb.Table;
import tinydb.operator.CrossProduct;
import tinydb.operator.Operator;
import tinydb.operator.Printer;
import tinydb.operator.Projection;
import tinydb.operator.Selection;
import tinydb.operator.Tablescan;

public class Executor {

	private Database db;

	public Executor(Database db){
		this.db = db;
	}
	
	public void execute(QueryPlan queryPlan) throws Exception{
		// handle relations
		Map<String, Table> h_rel = new HashMap<String, Table>();
		Map<String, Tablescan> h_scans = new HashMap<String, Tablescan>();
		for(String s : queryPlan.relations){
			String[] binding = s.split(" ");
			String tablename = binding[0];
			String bindingname = binding[1];
			Table table = db.getTable(tablename);
			if(table == null){
				System.err.println("Table "+tablename+" doesn't exist");
				throw new Exception();
			}
			h_rel.put(bindingname, table);
			Tablescan tablescan = new Tablescan(table);
			if(!h_scans.containsKey(bindingname)){
				h_scans.put(bindingname, tablescan);
			}
		}
		
		
		// handle conditions
		List<Condition> cond_join = new ArrayList<Condition>();
		Map<String, List<Condition>> cond_const = new HashMap<String, List<Condition>>();
		for(String cond : queryPlan.conditions){
			String expr[] = cond.split("=");
			String sa = expr[0];
			String sb = expr[1];
			// left side of condition
			String[] a_binding = sa.split("\\.");
			Table t_a = h_rel.get(a_binding[0]);
			if(t_a == null){
				System.err.println("There is no table for binding "+a_binding[0]+" in condition "+cond);
				throw new Exception();
			}
			int a_attr = t_a.findAttribute(a_binding[1]);
			if(a_attr == -1){
				System.err.println("There is no attribute "+a_binding[1]+" for binding "+a_binding[0]+" in condition "+cond);
				throw new Exception();
			}
			Register a = h_scans.get(a_binding[0]).getOutput()[a_attr];
			// right side of condition
			Register b;
			if(sb.contains(".")){
				String[] b_binding = sb.split("\\.");
				Table t_b = h_rel.get(b_binding[0]);
				if(t_b == null){
					System.err.println("There is no table for binding "+b_binding[0]+" in condition "+cond);
					throw new Exception();
				}
				int b_attr = t_b.findAttribute(b_binding[1]);
				if(b_attr == -1){
					System.err.println("There is no attribute "+b_binding[1]+" for binding "+b_binding[0]+" in condition "+cond);
					throw new Exception();
				}
				b = h_scans.get(b_binding[0]).getOutput()[b_attr];
				cond_join.add(new Condition(a, b));
			}else{
				// constant
				try{
					int i = Integer.parseInt(sb);
					b = new Register(i);
				}catch(NumberFormatException e){
					b = new Register(sb);
				}
				
				if(!cond_const.containsKey(a_binding[0])){
					cond_const.put(a_binding[0], new ArrayList<Condition>());
				}
				cond_const.get(a_binding[0]).add(new Condition(a, b));
			}
		}
		
		
		// handle selections
		// accumulator for cross products
		Operator cp = null;
		// push selections with constants down to base relations
		for(Entry<String, Tablescan> e : h_scans.entrySet()){
			Operator op = e.getValue();
			if(cond_const.containsKey(e.getKey())){
				for(Condition cond : cond_const.get(e.getKey())){
					op = new Selection(op, cond.a, cond.b);
				}
			}
			if(cp == null){
				cp = op;
			}else{
				cp = new CrossProduct(cp, op);
			}
		}
		// do join conditions from on cross product
		Operator select = cp;
		for(Condition cond : cond_join){
			select = new Selection(select, cond.a, cond.b);
		}
		
		
		// handle projections
		List<Register> a_proj = new ArrayList<Register>();
		if(!queryPlan.star){
			// check if attributes exist. problem: on which table? binding missing in definition?
			// -> go through all tables for every attribute
			attrloop:
			for(String attr : queryPlan.attributes){
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
}
