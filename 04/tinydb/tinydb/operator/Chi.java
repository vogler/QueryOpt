package tinydb.operator;

import tinydb.Register;

/** A chi operator */
public class Chi implements Operator
{
   /** A operation */
   public interface Operation {
      Object calc(Object a,Object b);
   }
   /** Add */
   public static class Add implements Operation {
      public Object calc(Object a,Object b) {
	 if (a instanceof Integer) {
	    return new Integer(((Integer)a).intValue()+((Integer)b).intValue());
	 } else if (a instanceof Double) {
	    return new Double(((Double)a).doubleValue()+((Double)b).doubleValue());
	 } else return null;
      }
   }
   /** Divide */
   public static class Div implements Operation {
      public Object calc(Object a,Object b) {
	 if (a instanceof Integer) {
	    return new Integer(((Integer)a).intValue()/((Integer)b).intValue());
	 } else if (a instanceof Double) {
	    return new Double(((Double)a).doubleValue()/((Double)b).doubleValue());
	 } else return null;
      }
   }
   /** Compare */
   public static class Equal implements Operation {
      public Object calc(Object a,Object b) {
	 if ((a==null)||(b==null))
	    return null;
	 return new Boolean(a.equals(b));
      }
   }
   /** Compare */
   public static class NotEqual implements Operation {
      public Object calc(Object a,Object b) {
	 if ((a==null)||(b==null))
	    return null;
	 return new Boolean(!a.equals(b));
      }
   }
   /** Compare */
   public static class Less implements Operation {
      public Object calc(Object a,Object b) {
	 if ((a==null)||(b==null))
	    return null;
	 return new Boolean(((Comparable)a).compareTo((Comparable)b)<0);
      }
   }
   /** Compare */
   public static class LessOrEqual implements Operation {
      public Object calc(Object a,Object b) {
	 if ((a==null)||(b==null))
	    return null;
	 return new Boolean(((Comparable)a).compareTo((Comparable)b)<0);
      }
   }

   /** The input */
   private final Operator input;
   /** The operation */
   private final Operation op;
   /** The input of the operation */
   private final Register left,right;
   /** The output of the operation */
   private final Register output;

   /** Constructor.
     * Sample usage: new Chi(operator,new Chi.Add(),reg1,reg2);
     */
   public Chi(Operator input,Operation op,Register left,Register right) {
      this.input=input;
      this.op=op;
      this.left=left;
      this.right=right;
      output=new Register();
   }

   /** Open the operator */
   public void open() { input.open(); }
   /** Get the next tuple */
   public boolean next() throws java.io.IOException {
      // Produce a tuple
      if (!input.next())
	 return false;
      // Calculate the value
      output.value=op.calc((left!=null)?left.value:null,(right!=null)?right.value:null);

      return true;
   }
   /** Close the operator */
   public void close() { input.close(); }

   /** Get the produced value */
   public Register getResult() { return output; }
   /** Get all produced values */
   public Register[] getOutput() {
      Register[] i=input.getOutput();
      Register[] o=new Register[i.length+1];
      System.arraycopy(i,0,o,0,i.length);
      o[i.length]=output;
      return o;
   }
}
