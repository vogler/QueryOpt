package parser;



public class Executor {
	
	public static void execute(QueryPlan queryPlan) throws Exception{
		long start = System.currentTimeMillis();
		queryPlan.execute();
		long end = System.currentTimeMillis();
		System.out.println("Execution time: "+(end-start)+"ms");
	}
}
