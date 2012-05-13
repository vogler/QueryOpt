package parser;

import java.util.ArrayList;
import java.util.List;


public class PairRelation {
	public String table;
	public String binding;

	public PairRelation(String table, String binding) {
		this.table = table;
		this.binding = binding;
	}
	
	public static List<PairRelation> parse(List<String> list){
		ArrayList<PairRelation> r = new ArrayList<PairRelation>();
		List<String> bindings = new ArrayList<String>();
		for(String s : list){
			String[] a = s.split(" ");
			if(bindings.contains(a[1].trim())){
				System.err.println("Bindings have to be unique! Can't use "+a[1].trim()+" for table "+a[0].trim());
			}
			bindings.add(a[1].trim());
			r.add(new PairRelation(a[0].trim(), a[1].trim()));
		}
		return r;
	}
	
	@Override
	public String toString(){
		return table+" "+binding;
	}
}
