package tinydb.operator;

import tinydb.Register;

/** A projection */
public class Projection implements Operator
{
   /** The input */
   private final Operator input;
   /** The output */
   private final Register[] output;

   /** Constructor */
   public Projection(Operator input,Register[] output) {
      this.input=input;
      this.output=output;
   }

   /** Open the operator */
   public void open() { input.open(); }
   /** Get the next tuple */
   public boolean next() throws java.io.IOException {
      return input.next();
   }
   /** Close the operator */
   public void close() { input.close(); }

   /** Get all produced values */
   public Register[] getOutput() { return output; }
}
