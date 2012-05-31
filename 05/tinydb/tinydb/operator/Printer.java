package tinydb.operator;

import tinydb.Register;

/** Prints tuple attributes */
public class Printer implements Operator
{
   /** The input */
   private final Operator input;
   /** Registers to print */
   private Register[] toPrint;

   /** Constructor */
   public Printer(Operator input) {
      this.input=input;
      toPrint=input.getOutput();
   }
   /** Constructor */
   public Printer(Operator input,Register[] toPrint) {
      this.input=input;
      this.toPrint=toPrint;
   }

   /** Open the operator */
   public void open() { input.open(); }
   /** Get the next tuple */
   public boolean next() throws java.io.IOException {
      // Produce a tuple
      if (!input.next())
         return false;
      // Print entries
      for (int index=0;index<toPrint.length;index++) {
	 if (index>0) System.out.print(' ');
	 if (toPrint[index].value==null)
	    System.out.print("null"); else
	    System.out.print(toPrint[index].value.toString());
      }
      System.out.println();

      return true;
   }
   /** Close the operator */
   public void close() { input.close(); }

   /** Get all produced values */
   public Register[] getOutput() { return input.getOutput(); }
}
