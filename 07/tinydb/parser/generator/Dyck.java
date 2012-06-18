package parser.generator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class Dyck {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		test(0, 4);
		test(1, 4);
		test(2, 4);
		test(56, 8);
	}
	
	private static void test(int i, int n) {
		System.out.println("i: "+i+", n: "+n+" -> "+unrankString(i, n));
	}
	
	public static boolean[] unrank(int r, int n){
		String s = unrankString(r, n);
		boolean[] b = new boolean[s.length()];
		for(int i=0; i<s.length(); i++){
			b[i] = s.charAt(i) == '1';
		}
		return b;
	}

	public static String unrankString(int r, int n){
		int open = 1;
		int close = 0;
		int pos = 1;
		char[] encoding = new char[n*2];
		encoding[0] = '1';
		while(pos < n*2){
			int k = q(open+close, open-close, n-1);
			if(k <= r){ //  && close < n
				r -= k;
				close++;
				encoding[pos] = '0';
			}else{
				open++;
				encoding[pos] = '1';
			}
			pos++;
		}
		return String.valueOf(encoding);
	}

	private static int q(int i, int j, int n) {
		if(i==0 && j==0){
			System.err.println("not implemented, but also not necessary");
//			return 0; // TODO C(n)
		}
		int r = p(2*n-i, j);
//		System.err.println("n="+n+", i="+i+", j="+j+" -> q="+r);
		return r;
	}

	public static int p(double i, double j){
		return (int) ((j+1)*Dyck_jo.binCoeff(i+1, 0.5*(i+j)+1)/(i+1));
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
    }
	
//	private static int p(double i, double j) {
//		return (int) ((j+1)/(i+1)*binom((int) (i+1), (int) (0.5*(i+j)+1)));
//	}
//	
//	// Binomial coefficient using DP 
//	private static int binom(int n, int m){
//		int[] b = new int[n+1];
//		b[0] = 1;
//		for(int i=1; i<=n; ++i){
//			b[i] = 1;
//			for(int j=i-1; j>0; --j){
//				b[j] += b[j-1];
//			}
//		}
////		System.out.println(m+" out of "+n+" = "+b[m]);
//		return b[m];
//	}

	
	public static int catalan(int n){
		return (int) (binCoeff(2*n, n)-binCoeff(2*n, n+1));
	}

	public static <T> List<T> unrankPermutation(Collection<T> elements, int r) {
		List<T> pi = new ArrayList<T>(elements);
		for(int i=elements.size(); i>0; i--){
			Collections.swap(pi, i-1, r%i);
			r = r/i;
		}
		return pi;
	}
}
