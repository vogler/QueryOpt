package tinydb.operator;

import tinydb.Register;

/** A selection */
public class Selection implements Operator
{
   /** The input */
   private final Operator input;
   /** Registers of the condition */
   private final Register condition;
   /** Second register for implicit equal tests */
   private final Register equal;

   /** Constructor. Condition must be a boolean value */
   public Selection(Operator input,Register condition) {
      this.input=input;
      this.condition=condition;
      equal=null;
   }
   /** Constructor. Registers a and b are compared */
   public Selection(Operator input,Register a,Register b) {
      this.input=input;
      condition=a;
      equal=b;
   }

   /** Open the operator */
   public void open() { input.open(); }
   /** Get the next tuple */
   public boolean next() throws java.io.IOException {
      while (true) {
	 // Produce a tuple
	 if (!input.next())
	    return false;
	 // Check
	 if (equal!=null) {
	    if (condition.value!=null) {
	       if (condition.value.equals(equal.value))
	          return true;
	    }
	 } else {
	    if (Boolean.TRUE.equals(condition.value))
	       return true;
	 }
      }
   }
   /** Close the operator */
   public void close() { input.close(); }

   /** Get all produced values */
   public Register[] getOutput() { return input.getOutput(); }
}
