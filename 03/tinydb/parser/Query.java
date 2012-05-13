package parser;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Query {
	
	public List<String> attributes;
	public List<PairRelation> relations;
	public List<PairCondition> conditions;
	public boolean star = false;

	private Query(List<String> attributes, List<PairRelation> relations, List<PairCondition> conditions){
		this.attributes = attributes;
		this.relations = relations;
		this.conditions = conditions;
		this.star = attributes.size() == 1 && attributes.get(0).equals("*");
	}

	public static Query parse(String q) throws Exception {
		Pattern p = Pattern.compile("select (.+) from (.+) where (.+)");
		Matcher m = p.matcher(q);
		if(!m.matches()){
			System.err.println("Query doesn't match the pattern: "+p.toString());
			throw new Exception();
		}
		String selections = m.group(1);
		String relations = m.group(2);
		String joincond = m.group(3);
		// (*|attribute(,attribute)*)
		p = Pattern.compile("\\*|\\w+(\\s*,\\s*\\w+)*");
		if(!p.matcher(selections).matches()){
			System.err.println("Selections don't match the pattern: "+p.toString());
			System.err.println("Your input: "+selections);
			throw new Exception();
		}
		// relation binding(,relation binding)*
		p = Pattern.compile("\\w+ \\w+(\\s*,\\s*\\w+ \\w+)*");
		if(!p.matcher(relations).matches()){
			System.err.println("Relations don't match the pattern: "+p.toString());
			System.err.println("Your input: "+relations);
			throw new Exception();
		}
		// binding.attribute=(binding.attribute|constant)
		// (and binding.attribute=(binding.attribute|constant))*
		p = Pattern.compile("\\w+\\.\\w+=(\\w+\\.\\w+|\"?\\w+\"?)( and \\w+\\.\\w+=(\\w+\\.\\w+|\"?\\w+\"?))*");
		if(!p.matcher(joincond).matches()){
			System.err.println("Join conditions don't match the pattern: "+p.toString());
			System.err.println("Your input: "+joincond);
			throw new Exception();
		}
		
		// store the query structure (to file?)
		BufferedWriter bw = new BufferedWriter(new FileWriter("query.txt"));
		bw.write("relations: "+relations);
		bw.newLine();
		bw.write("selections: "+selections);
		bw.newLine();
		bw.write("joinconditions: "+joincond);
		bw.close();
		
		Query query = new Query(splitAndTrim(selections, ","), PairRelation.parse(splitAndTrim(relations, ",")),
				PairCondition.parse(splitAndTrim(joincond, " and ")));
		return query;
	}
	
	private static List<String> splitAndTrim(String s, String delim){
		List<String> l = new ArrayList<String>();
		String[] a = s.split(delim);
		for(String x : a){
			l.add(x.trim());
		}
		return l;
	}

}
