package parser;

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
import tinydb.operator.Projection;
import tinydb.operator.Selection;
import tinydb.operator.Tablescan;


public class PlanGenerator {

	private Database db;
	private Map<String, Table> h_tables;
	private Map<String, Tablescan> h_scans;

	public PlanGenerator(Database db) {
		this.db = db;
	}

	public QueryPlan parse(Query q) throws Exception {
		h_tables = new HashMap<String, Table>();
		h_scans = new HashMap<String, Tablescan>();
		for(PairRelation r : q.relations){
			Table table = db.getTable(r.table);
			if(table == null){
				System.err.println("Table "+r.table+" doesn't exist");
				throw new Exception();
			}
			h_tables.put(r.binding, table);
			if(!h_scans.containsKey(r.binding)){
				Tablescan tablescan = new Tablescan(table);
				h_scans.put(r.binding, tablescan);
			}
		}
		
		
		// handle conditions
		List<Condition> cond_join = new ArrayList<Condition>();
		Map<String, List<Condition>> cond_const = new HashMap<String, List<Condition>>();
		for(PairCondition c : q.conditions){
			// left side of condition
			Register a = getRegister(c.a, c);
			// right side of condition
			Register b = null;
			if(c.b.contains(".")){
				b = getRegister(c.b, c);
				cond_join.add(new Condition(a, b));
			}else{
				// constant
				if(c.b.matches("\\d*\\.?\\d*")){ // 1, 1., .1, 1.1, . :(
					try{
						// first try double then integer, if integer fails double should be kept, otherwise use string
						b = new Register(Double.parseDouble(c.b));
						b = new Register(Integer.parseInt(c.b));
					}catch(NumberFormatException e){
						// string
						if(b==null){
							b = new Register(c.b);
						}
					}
				}else{
					// string
					Matcher m = Pattern.compile("'(.*)'|\"(.*)\"").matcher(c.b);
					b = new Register(m.matches() ? m.group(1) != null ? m.group(1) : m.group(2) : c.b);
				}
				String a_binding = c.a.split("\\.")[0];
				if(!cond_const.containsKey(a_binding)){ // no conditions for binding yet
					cond_const.put(a_binding, new ArrayList<Condition>());
				}
				cond_const.get(a_binding).add(new Condition(a, b));
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
				cp = op; // first selection
			}else{
				cp = new CrossProduct(cp, op);
			}
		}
		// do join conditions on cross product
		Operator select = cp;
		for(Condition cond : cond_join){
			select = new Selection(select, cond.a, cond.b);
		}
		
		
		// handle projections
		List<Register> a_proj = new ArrayList<Register>();
		if(!q.star){
			// check if attributes exist. problem: on which table? binding missing in definition?
			// -> go through all tables for every attribute
			attrloop:
			for(String attr : q.attributes){
				for(Entry<String, Table> e : h_tables.entrySet()){
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
		
		return new QueryPlan(select);
	}
	
	
	private Register getRegister(String s, PairCondition c) throws Exception{
		String[] a = s.split("\\.");
		String binding = a[0];
		String attribute = a[1];
		Table table = h_tables.get(binding);
		if(table == null){
			System.err.println("There is no table for binding "+binding+" in condition "+c);
			throw new Exception();
		}
		int iattr = table.findAttribute(attribute);
		if(iattr == -1){
			System.err.println("There is no attribute "+attribute+" for binding "+binding+" in condition "+c);
			throw new Exception();
		}
		Register r = h_scans.get(binding).getOutput()[iattr];
		return r;
	}
}
