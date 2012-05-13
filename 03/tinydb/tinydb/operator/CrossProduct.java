package tinydb.operator;

import tinydb.Register;

/** A cross product */
public class CrossProduct implements Operator
{
   /** The input */
   private final Operator left,right;
   /** Read the left side? */
   private boolean readLeft;

   /** Constructor */
   public CrossProduct(Operator left,Operator right) {
      this.left=left;
      this.right=right;
      readLeft=true;
   }

   /** Open the operator */
   public void open() { left.open(); readLeft=true; }
   /** Get the next tuple */
   public boolean next() throws java.io.IOException {
      while (true) {
	 // Read the left side?
	 if (readLeft) {
	    if (!left.next())
	       return false;
	    readLeft=false;
	    right.open();
	 }
	 // Read the right side
	 if (!right.next()) {
	    right.close();
	    readLeft=true;
	    continue;
	 }
	 // Got a pair
	 return true;
      }
   }
   /** Close the operator */
   public void close() { if (!readLeft) { right.close(); readLeft=true; } left.close(); }

   /** Get all produced values */
   public Register[] getOutput() {
      Register[] l=left.getOutput();
      Register[] r=right.getOutput();
      Register[] o=new Register[l.length+r.length];
      System.arraycopy(l,0,o,0,l.length);
      System.arraycopy(r,0,o,l.length,r.length);
      return o;
   }
}
