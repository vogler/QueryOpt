package tinydb.operator;

import java.util.*;
import tinydb.Register;

/** A hash join */
public class HashJoin implements Operator
{
   /** The input */
   private final Operator left,right;
   /** The registers */
   private final Register leftValue,rightValue;
   /** The copy mechanism */
   private final Register[] leftRegs;
   /** The hashtable */
   private Map table;
   /** Iterator */
   private Iterator iter;

   /** Constructor */
   public HashJoin(Operator left,Operator right,Register leftValue,Register rightValue) {
      this.left=left;
      this.right=right;
      this.leftValue=leftValue;
      this.rightValue=rightValue;
      this.leftRegs=left.getOutput();
   }
   /** Store the left hand side */
   private Object[] storeLeft() {
      Object[] values=new Object[leftRegs.length];
      for (int index=0;index<values.length;index++)
	 values[index]=leftRegs[index].value;
      return values;
   }
   /** Restore the left hand side */
   private void restoreLeft(Object[] values) {
      for (int index=0;index<values.length;index++)
	 leftRegs[index].value=values[index];
   }

   /** Open the operator */
   public void open() { left.open(); right.open(); table=null; }
   /** Get the next tuple */
   public boolean next() throws java.io.IOException {
      // First pass? Hash the left side
      if (table==null) {
	 table=new HashMap();
	 while (left.next()) {
	    Object[] values=storeLeft();
            Object o=table.get(leftValue.value);
            if (o==null) {
	       table.put(leftValue.value,values);
	    } else if (o instanceof List) {
	       ((List)o).add(values);
	    } else {
	       LinkedList list=new LinkedList();
	       list.add(o);
	       list.add(values);
	       table.put(leftValue.value,list);
	    }
	 }
      }
      // More matches?
      if (iter!=null) {
	 if (iter.hasNext()) {
	    restoreLeft((Object[])iter.next());
	    return true;
         }
         iter=null;
      }
      // Read the right hand side
      while (true) {
	 // Read the right side
	 if (!right.next()) {
	    right.close();
	    return false;
	 }
	 // Probe the hash table
	 Object o=table.get(rightValue.value);
	 if (o==null) continue;
	 if (o instanceof List) {
	    iter=((List)o).iterator();
	    restoreLeft((Object[])iter.next());
	    return true;
	 } else {
	    restoreLeft((Object[])o);
	    return true;
	 }
      }
   }
   /** Close the operator */
   public void close() { if (table!=null) right.close(); table=null; left.close(); }

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
