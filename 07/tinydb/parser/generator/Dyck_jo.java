package parser.generator;
import java.util.LinkedList;


public class Dyck_jo {

	public static void main(String[] args) {
//		test(4, 0);
//		test(4, 1);
//		test(4, 2);
		test(8, 56);
		//System.out.println(Dyck_jo.q(0, 0, 8));
	}
	
	private static void test(int n, int i) {
		System.out.print("n: "+n+", i: "+i+" -> ");
		LinkedList<Integer> t = unrankTree(n,i);
		for (int e : t){
			System.out.print(e+" ");
		}
		System.out.println();
	}
	
	public static LinkedList<Integer> unrankTree(double n, double i){
		int open = 1;
		int close = 0;
		int pos = 1;
		LinkedList<Integer> encoding = new LinkedList<Integer>();
		encoding.add(1);
		//n--;n--;
		double n_t = n-1;
		while (encoding.size()<n){
			
			//System.out.println("open: "+open+" close: "+close+" pos: "+pos);
			double k = q(open+close, open-close, n_t);
			System.out.print("q: "+k);
			if (k<=i){//rank is bigger than q: step down, rank = rank-q
				System.out.println(", down!");
				i=i-k;
				close++;
			} else {//rank is smaller than q: step up
				System.out.println(", up!");
				encoding.add(pos+1);
				open++;
			}
			pos++;
		}
		return encoding;
	}
	
	//number of different paths from (i,j) to (2n,0)
	public static double q(double i, double j, double n){
		System.out.println("called q with i="+i+", j="+j+", n="+n);
		return Dyck_jo.p(2*n-i, j);
	}
	
	//number of different paths from (0,0) to (i,j)
	public static double p(double i, double j){
		//return (j+1)*Dyck_jo.binCoeff(i+1, Math.round(0.5*(i+j)+1))/(i+1);
		return (j+1)*Dyck_jo.binCoeff(i+1, 0.5*(i+j)+1)/(i+1);
	}
	
	//binomial coefficient
	public static double binCoeff(double n, double k){
		return Dyck_jo.fac(n)/(Dyck_jo.fac(k)*Dyck_jo.fac(n-k));
	}
	
	//factorial
	public static double fac(double n)
    {
		if (n==0) return 1;
		double ret = n;
		for (double i = n-1;i>0;--i){
			ret = ret*i;
		}
		return ret;
//        if (n == 0) return 1;
//        return n * fac(n-1);
    }

}
