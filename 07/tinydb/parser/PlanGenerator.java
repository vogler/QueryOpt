package parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import parser.generator.Dyck;
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


public class PlanGenerator {

	private Database db;
	private Query query;
	private Map<String, Table> h_tables;
	private Map<String, Tablescan> h_scans;
	private Operator select;
	private List<Condition> cond_join = new ArrayList<Condition>();
	private Map<String,Double> cardinalities = new HashMap<String,Double>();
	private Map<String,Double> costs = new HashMap<String,Double>();
	private Map<Condition, Double> selectivities = new HashMap<Condition, Double>();
	// query plan
	private List<String> plan = new ArrayList<String>();
	// query graph
	private List<Node> nodes = new ArrayList<Node>();
	private List<Edge> edges = new ArrayList<Edge>();
	// join connected components
	private Map<String, Set<String>> connectedBindings = new HashMap<String, Set<String>>();
	// will contain all scans + pushed down selections
	private Map<String, Operator> h_selections = new HashMap<String, Operator>();
	// connected components
	private Map<String, Operator> connectedComp = new HashMap<String, Operator>();
	private List<Condition> usedConditions = new ArrayList<Condition>(); // TODO: just for random
	
	
	public PlanGenerator(Database db) {
		this.db = db;
	}

	public QueryPlan parse(Query q) throws Exception {
		this.query = q;
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
			costs.put(r.binding, 0.0);
			if(!h_scans.containsKey(r.binding)){ // TODO enough to save per table instead of binding?
				Tablescan tablescan = new Tablescan(table);
				h_scans.put(r.binding, tablescan);
			}
		}
		
		
		// handle conditions
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
		
		
		// handle selections
		// push selections with constants down to base relations
		h_selections = new HashMap<String, Operator>(h_scans);
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
			cardinalities.put(e.getKey(), cardinality);
			costs.put(e.getKey(), 0.0);
			nodes.add(new Node(q.getRelation(e.getKey()), pushedCond, (int) Math.ceil(cardinality)));
		}
		
		
		// calculate selectivity for all joins
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
		
		// compute connected bindings
		for(Condition cond : cond_join){
			PairCondition bindings = cond.pair.getBindings();
			// update list of connected bindings and components
			if(!connectedBindings.containsKey(bindings.a)){
				connectedBindings.put(bindings.a, new HashSet<String>());
			}
			if(!connectedBindings.containsKey(bindings.b)){
				connectedBindings.put(bindings.b, new HashSet<String>());
			}
			connectedBindings.get(bindings.a).add(bindings.b);
			connectedBindings.get(bindings.b).add(bindings.a);
		}
		
		
		// GOO
//		goo();
		// TODO: DP
		
		// Random
		List<String> prevplan = new ArrayList<String>(plan);
		int mincost = randomTree(); // init with first random tree
		Operator minselect = select;
		List<String> minplan = new ArrayList<String>(plan);
		List<Edge> minedges = edges;
		for(int i=1; i<100; i++){ // try 100 random trees in total an take the best one
			select = null;
			plan = new ArrayList<String>();
//			edges = new ArrayList<Edge>();
			int cost = randomTree();
			if(cost < mincost){
				mincost = cost;
				minselect = select;
				minplan = plan;
				minedges = edges;
			}
		}
		// restore best one
		select = minselect; prevplan.addAll(minplan); plan = prevplan; edges = minedges;
		
		
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
	
	class Tree<T> {
		T value;
		Tree<T> left;
		Tree<T> right;
		int costs;
		
		Tree(Tree<T> left, Tree<T> right, T value){
			this.left = left;
			this.right = right;
			this.value = value;
		}
		
		int size(){
			if(left == null && right == null) // leaf
				return 1;
			return 1+left.size()+right.size();
		}
		
		int nleaves(){
			if(left == null && right == null) // leaf
				return 1;
			return left.nleaves()+right.nleaves();	
		}
		
		List<T> values(){
			List<T> list = new ArrayList<T>();
			if(left == null && right == null) // leaf
				list.add(value);
			else{
				list.addAll(left.values());
				list.addAll(right.values());
			}
			return list;
		}
		
		public String toString(){
			String r = " ";
			if(left != null && right != null){
				r += "|><|";
				r += left.toString();
				r += right.toString();
			}else{
				r += value;
			}
			return r;
		}
	}
	
	// random join tree generation
	private int randomTree() {
		int n = query.relations.size();
		Random rand = new Random();
		// 1. generate a random number b in [0, C(n)[
		int b = rand.nextInt(Dyck.catalan(n-1));
		// 2. unrank b to obtain a bushy tree with n-1 inner nodes
		boolean[] tree = Dyck.unrank(b, n-1);
		// 3. generate a random number p in [0, n![
		int p = rand.nextInt((int) Dyck.fac(n));
		// 4. unrank p to obtain a permutation
		List<String> leaves = Dyck.unrankPermutation(h_scans.keySet(), p);
		// 5. attach the relations in order p from left to right as leaf nodes to the binary tree obtained in step 2
		List<Boolean> encoding = new ArrayList<Boolean>();
		for(boolean bool : tree){
			encoding.add(bool);
		}
		Tree<String> root = createTree(encoding, leaves);
		System.out.println(root.toString());
		select = joinOrCross(root);
		System.out.println("Generated random tree with costs "+root.costs);
		return root.costs;
	}

	private Operator joinOrCross(Tree<String> node) {
		if(node.value != null){ // leaf
			plan.add("Get selection/tablescan for binding "+node.value);
			return h_selections.get(node.value);
		}
		Operator left = joinOrCross(node.left);
		Operator right = joinOrCross(node.right);
		// is there a join predicate that contains any relations from the two subtrees?
		for(Condition cond : cond_join){
			if(usedConditions.contains(cond)) continue;
			PairCondition bindings = cond.pair.getBindings();
			if(node.left.values().contains(bindings.a) && node.right.values().contains(bindings.b)
			|| node.left.values().contains(bindings.b) && node.right.values().contains(bindings.a)){ // connected component?
				Double c_a = cardinalities.get(bindings.a);
				Double c_b = cardinalities.get(bindings.b);
				double tmp = selectivities.get(cond)*c_a*c_b;
				node.costs = (int) (tmp + node.left.costs + node.right.costs);
				edges.add(new Edge(cond.pair, selectivities.get(cond)));
				usedConditions.add(cond);
				plan.add("HashJoin "+bindings.a+" & "+bindings.b+" with "+cond.pair+" and cost of "+node.costs);
				return new HashJoin(left, right, cond.a, cond.b);
			}
		}
		// otherwise do a cross product
		plan.add("CrossProduct "+left+" & "+right);
		return new CrossProduct(left, right);
	}

	private <T> Tree<T> createTree(List<Boolean> encoding, List<T> leaves) {
		Boolean current = encoding.size() > 0 ? encoding.remove(0) : false;
		if(current){
			Tree<T> left = createTree(encoding, leaves);
//			for(int i=0; i<left.size(); i++){ // shift input encoding by #elements in left subtree
//				encoding.remove(0);
//			}
//			for(int i=0; i<left.nleaves(); i++){ // shift leaves of left subtree
//				leaves.remove(0);
//			}
			Tree<T> right = createTree(encoding, leaves);
			return new Tree<T>(left, right, null);
		}else{
			T leaf = leaves.remove(0);
			return new Tree<T>(null, null, leaf);
		}
	}

	// greedy operator ordering
	private void goo() {
		while (!cond_join.isEmpty()){ 	//repeat until cond_join is empty
			Condition cond_min = null;
			double card_min = Double.MAX_VALUE;
			double cost_min = 0;
			//Pair min = new Pair(null, Double.MAX_VALUE);
			double c_a;
			double c_b;
			//calculate intermediate results for all possible remaining joins
			for(Condition cond : cond_join){
				PairCondition bindings = cond.pair.getBindings();
				c_a = cardinalities.get(bindings.a);
				c_b = cardinalities.get(bindings.b);
				double tmp = selectivities.get(cond)*c_a*c_b;
				System.out.println(bindings.a+" & "+bindings.b+" with "+cond.pair+" has cardinality of "+tmp+
				" ("+selectivities.get(cond)+"*"+c_a+"*"+c_b+")");
				
				if (tmp<card_min){
					card_min = tmp;
					cond_min = cond;
					cost_min = tmp+costs.get(bindings.a)+costs.get(bindings.b);
				}
				
			}
			//pick minimal int. result, remove join from cond_join
			Condition cond = cond_min;
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
			// update costs for left and right
			costs.put(bindings.a, cost_min);
			costs.put(bindings.b, cost_min);
			for(String s : connectedBindings.get(bindings.a)){
				connectedComp.put(s, select);
				cardinalities.put(s, card_min);
				costs.put(s, cost_min);
			}
			for(String s : connectedBindings.get(bindings.b)){
				connectedComp.put(s, select);
				cardinalities.put(s, card_min);
				costs.put(s, cost_min);
			}
			plan.add("HashJoin "+bindings.a+" & "+bindings.b+" with "+cond.pair+" and cost of "+cost_min);
			edges.add(new Edge(cond.pair, selectivities.get(cond)));
		}

		
		// use cross product to join connected components
		select = null;
		for(Operator op : new HashSet<Operator>(connectedComp.values())){ // gets distinct connected components from map
			if(select == null){
				select = op;
			}else{
				select = new CrossProduct(select, op);
				plan.add("CrossProduct "+select+" & "+op);
			}
		}
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
