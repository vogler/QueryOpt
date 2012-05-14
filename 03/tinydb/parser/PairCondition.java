package parser;

import java.util.ArrayList;
import java.util.List;


public class PairCondition {
	public String a;
	public String b;

	public PairCondition(String a, String b) {
		this.a = a;
		this.b = b;
	}
	
	public static List<PairCondition> parse(List<String> list){
		ArrayList<PairCondition> r = new ArrayList<PairCondition>();
		for(String s : list){
			String[] a = s.split("=");
			r.add(new PairCondition(a[0].trim(), a[1].trim()));
		}
		return r;
	}
	
	@Override
	public String toString(){
		return a+"="+b;
	}
	
	public PairCondition getBindings(){
		return new PairCondition(a.split("\\.")[0], b.split("\\.")[0]);
	}
}
