package parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import parser.querygraph.Edge;
import parser.querygraph.Node;
import tinydb.Attribute;
import tinydb.Database;
import tinydb.Register;
import tinydb.Table;
import tinydb.operator.CrossProduct;
import tinydb.operator.HashJoin;
import tinydb.operator.Operator;
import tinydb.operator.Projection;
import tinydb.operator.Selection;
import tinydb.operator.Tablescan;


public class PlanGeneratorDP {

	private Database db;
	private Map<String, Table> h_tables;
	private Map<String, Tablescan> h_scans;

	public PlanGeneratorDP(Database db) {
		this.db = db;
	}
	
	static private String bin_helper(TreeSet s, int l){
		String ret = new String();
		for (int i=0;i<l;i++){
			if (s.contains(i)) ret=ret+"1";
			else ret = ret+"0";
		}
		return ret;
	}
	
	static private String bin_helper(int s, int l){
		String ret = new String();
		for (int i=0;i<l;i++){
			if (i==s) ret=ret+"1";
			else ret = ret+"0";
		}
		return ret;
	}

	public QueryPlan parse(Query q) throws Exception {
		Map<String,Double> cardinalities = new HashMap<String,Double>();
		h_tables = new HashMap<String, Table>();
		h_scans = new HashMap<String, Tablescan>();
		for(PairRelation r : q.relations){
			Table table = db.getTable(r.table);
			if(table == null){
				System.err.println("Table "+r.table+" doesn't exist");
				throw new Exception();
			}
			h_tables.put(r.binding, table);
			cardinalities.put(r.binding, (double) table.getCardinality());
			if(!h_scans.containsKey(r.binding)){ // TODO enough to save per table instead of binding?
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
			if(c.b.contains(".")){ // TODO string constant might contain .
				b = getRegister(c.b, c);
				cond_join.add(new Condition(a, b, c));
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
				cond_const.get(a_binding).add(new Condition(a, b, c));
			}
		}
		
		
		// query plan
		List<String> plan = new ArrayList<String>();
		// query graph
		List<Node> nodes = new ArrayList<Node>();
		List<Edge> edges = new ArrayList<Edge>();
		
		
		// handle selections
		Map<String, Operator> h_selections = new HashMap<String, Operator>(h_scans);
		Map<String, Operator> connectedComp = new HashMap<String, Operator>();
		
		// push selections with constants down to base relations
		for(Entry<String, Tablescan> e : h_scans.entrySet()){
			Table table = h_tables.get(e.getKey());
			Operator op = e.getValue();
			List<PairCondition> pushedCond = new ArrayList<PairCondition>();
			double cardinality = table.getCardinality();
			if(cond_const.containsKey(e.getKey())){
				for(Condition cond : cond_const.get(e.getKey())){
					op = new Selection(op, cond.a, cond.b);
					plan.add("Selection "+e.getKey()+" with "+cond.pair);
					pushedCond.add(cond.pair);
					Attribute attr = getAttribute(table, cond.pair.getAttributeName());
					if(attr != null){
						if(attr.getKey()){
							cardinality = 1;
						}else{
							cardinality /= attr.getUniqueValues(); // non-key case
						}
					}
				}
				h_selections.put(e.getKey(), op);
				connectedComp.put(e.getKey(), op);
			}
			cardinalities.remove(e.getKey());
			cardinalities.put(e.getKey(), cardinality);
			nodes.add(new Node(q.getRelation(e.getKey()), pushedCond, (int) Math.ceil(cardinality)));
		}
		
		// join connected components
		Map<String, Set<String>> connectedBindings = new HashMap<String, Set<String>>();
		
		//calculate selectivity for all joins
		Map<Condition, Double> selectivities = new HashMap<Condition, Double>();
		for(Condition cond : cond_join){
			PairCondition bindings = cond.pair.getBindings();
			// estimate selectivity for join predicate
			double selectivity = 1;
			Table table_a = h_tables.get(bindings.a);
			Table table_b = h_tables.get(bindings.b);
			Attribute attr_a = getAttribute(table_a, cond.pair.getAttributes().a);
			Attribute attr_b = getAttribute(table_b, cond.pair.getAttributes().b);
			if (attr_a == null || attr_b == null)
				continue;
			if (attr_a.getKey() && attr_b.getKey()) { // both keys
				selectivity = 1. / Math.max(table_a.getCardinality(), table_b.getCardinality());
			} else if (!attr_a.getKey() && !attr_b.getKey()) { // both not keys
				selectivity = 1. / Math.max(attr_a.getUniqueValues(), attr_b.getUniqueValues());
			} else { // exactly one key
				selectivity = 1. / (attr_a.getKey() ? table_a.getCardinality() : table_b.getCardinality());
			}
			selectivities.put(cond, selectivity);
		}
		
		Map<String,Double[]> dp_table = new HashMap<String,Double[]>();//contains for each key (binary number) the cost and estimated cardinality
		Map<String,String[]> join_table = new HashMap<String,String[]>();//contains for each key the keys that were used to create the tree
		int num_bindings = h_tables.keySet().size();
		for (int i = 1;i<Math.pow(2, num_bindings);i++){
			String bin = Integer.toBinaryString(i);
			String bin_padded = bin;
			while (bin_padded.length()<num_bindings) bin_padded = "0"+bin_padded;
			final TreeSet<Integer> join_relations = new TreeSet<Integer>();
			for (int j = num_bindings-bin.length();j<num_bindings;j++){
				if (bin.charAt(0)==49) join_relations.add(j);
				if (bin.length()>1) bin = bin.substring(1);
			}
			Double[] cost = new Double[2];
			if (join_relations.size()>1){
				//pick each relation and join it with the rest
				double min = Double.MAX_VALUE;
				String bin_min_a;
				String bin_min_b;
				for(int j : join_relations){
					TreeSet<Integer> tmp = join_relations;
					tmp.remove(j);
					String a = PlanGeneratorDP.bin_helper(j, num_bindings);
					String b = PlanGeneratorDP.bin_helper(tmp, num_bindings);
				}
				
				//pick the one with lowest cost
			} else {
				//put each relation into the table, check for constant selections
				cost[0] = 0.0;
				cost[1] = cardinalities.get(h_tables.keySet().toArray(new String[0])[join_relations.first()]);
			}
			dp_table.put(bin_padded,cost);
		}
		
		class Pair
		{  public Pair(Condition c, double s)
		   {  first = c;
		      second = s;
		   }
		   public Condition getFirst()
		   {  return first;
		   }
		   public double getSecond()
		   {  return second;
		   }

		   private Condition first;
		   private double second;
		}
		
		while (!cond_join.isEmpty()){ 	//repeat until cond_join is empty
			Pair min = new Pair(null, Double.MAX_VALUE);
			double c_a;
			double c_b;
			Operator o;
			//calculate intermediate results for all possible remaining joins
			for(Condition cond : cond_join){
				PairCondition bindings = cond.pair.getBindings();
				if (connectedComp.containsKey(bindings.a)){//TODO: determine cardinality of a join
					c_a = 0;
					o = connectedComp.get(bindings.a);
					o.open();
					while(o.next()) c_a++;
					o.close();
				} else {
					c_a = h_tables.get(bindings.a).getCardinality();
				}
				if (connectedComp.containsKey(bindings.b)){
					c_b = 0;
					o = connectedComp.get(bindings.b);
					o.open();
					while(o.next()) c_b++;
					o.close();
				} else {
					c_b = h_tables.get(bindings.b).getCardinality();
				}
//				Operator left = connectedComp.containsKey(bindings.a) ? connectedComp.get(bindings.a) : h_selections.get(bindings.a);
//				Operator right = connectedComp.containsKey(bindings.b) ? connectedComp.get(bindings.b) : h_selections.get(bindings.b);
				double tmp = selectivities.get(cond)*c_a*c_b;
				System.out.println(bindings.a+" & "+bindings.b+" with "+cond.pair+" has cost of "+min.getSecond()+
				" ("+selectivities.get(cond)+"*"+c_a+"*"+c_b+")");
				
				if (tmp<min.getSecond()) min = new Pair(cond, tmp);
			}
			//pick minimal int. result, remove join from cond_join
			Condition cond = min.getFirst();
			cond_join.remove(cond);
			//execute (TODO: improvements possible?)
			PairCondition bindings = cond.pair.getBindings();
			if(connectedComp.containsKey(bindings.a)){
				plan.add("Getting "+bindings.a+" from map of connected components/selections...");
			}
			if(connectedComp.containsKey(bindings.b)){
				plan.add("Getting "+bindings.b+" from map of connected components/selections...");
			}
			Operator left = connectedComp.containsKey(bindings.a) ? connectedComp.get(bindings.a) : h_selections.get(bindings.a);
			Operator right = connectedComp.containsKey(bindings.b) ? connectedComp.get(bindings.b) : h_selections.get(bindings.b);
			Operator select = new HashJoin(left, right, cond.a, cond.b);
			
			// update list of connected bindings and components
			if(!connectedBindings.containsKey(bindings.a)){
				connectedBindings.put(bindings.a, new HashSet<String>());
			}
			if(!connectedBindings.containsKey(bindings.b)){
				connectedBindings.put(bindings.b, new HashSet<String>());
			}
			connectedBindings.get(bindings.a).add(bindings.b);
			connectedBindings.get(bindings.b).add(bindings.a);
			for(String s : connectedBindings.get(bindings.a)){
				connectedComp.put(s, select);
			}
			for(String s : connectedBindings.get(bindings.b)){
				connectedComp.put(s, select);
			}
			plan.add("HashJoin "+bindings.a+" & "+bindings.b+" with "+cond.pair+" and cost of "+min.getSecond());
			// estimate selectivity for join predicate
			double selectivity = 1;
			Table table_a = h_tables.get(bindings.a);
			Table table_b = h_tables.get(bindings.b);
			Attribute attr_a = getAttribute(table_a, cond.pair.getAttributes().a);
			Attribute attr_b = getAttribute(table_b, cond.pair.getAttributes().b);
			if(attr_a == null || attr_b == null) continue;
			if(attr_a.getKey() && attr_b.getKey()){ // both keys
				selectivity = 1./Math.max(table_a.getCardinality(), table_b.getCardinality());
			}else if(!attr_a.getKey() && !attr_b.getKey()){ // both not keys
				selectivity = 1./Math.max(attr_a.getUniqueValues(), attr_b.getUniqueValues());
			}else{ // exactly one key
				selectivity = 1./(attr_a.getKey() ? table_a.getCardinality() : table_b.getCardinality());
			}
			edges.add(new Edge(cond.pair, selectivity));
		}

		
		// use cross product to join connected components
		Operator select = null;
		for(Operator op : new HashSet<Operator>(connectedComp.values())){ // gets distinct connected components from map
			if(select == null){
				select = op;
			}else{
				select = new CrossProduct(select, op);
				plan.add("CrossProduct "+select+" & "+op);
			}
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
						plan.add("Projection "+e.getKey()+"."+attr);
						continue attrloop;
					}
				}
			}
			// do projection
			Projection project = new Projection(select, a_proj.toArray(new Register[0]));
			select = project;
		}
		
		return new QueryPlan(select, plan, nodes, edges);
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
	
	private Attribute getAttribute(Table table, String attr){
		int iattr = table.findAttribute(attr);
		if(iattr == -1) return null;
		return table.getAttribute(iattr);
	}
}
