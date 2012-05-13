package tinydb.operator;

/** Operator interface */
public interface Operator
{
   /** Open the operator */
   void open();
   /** Produce the next tuple */
   boolean next() throws java.io.IOException;
   /** Close the operator */
   void close();

   /** Get all produced values */
   tinydb.Register[] getOutput();
}