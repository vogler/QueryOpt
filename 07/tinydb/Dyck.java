
public class Dyck {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		test(0, 4);
		test(1, 4);
		test(2, 4);
//		System.out.println(binom(1,2));
//		System.out.println(binom(2,3));
	}
	
	private static void test(int i, int n) {
		System.out.println("i: "+i+", n: "+n+" -> "+getDyckWord(i, n));
	}

	public static String getDyckWord(int r, int n){
		int open = 1;
		int close = 0;
		int pos = 1;
		char[] encoding = new char[n*2];
		encoding[0] = '1';
		while(pos < n*2){
			int k = q(open+close, open-close, n);
			if(k <= r){
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
		System.err.println(i+" "+j);
		if(j==0 && (i==0 || i==2*n)){
			System.err.println("not implemented yet");
			return 0; // TODO C(n)
		}
		return p(2*n-i, j);
	}

	private static int p(int i, int j) {
		return (j+1)/(i+1)*binom(i+1, (int) (0.5*(i+j)+1));
	}
	
	// Binomial coefficient using DP 
	private static int binom(int n, int m){
//		System.out.println(n+" out of "+m);
		int[] b = new int[n+1];
		b[0] = 1;
		for(int i=1; i<=n; ++i){
			b[i] = 1;
			for(int j=i-1; j>0; --j){
				b[j] += b[j-1];
			}
		}
		return b[m];
	}

}
