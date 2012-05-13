package tinydb;

/** A runtime register */
public class Register {
   /** The value */
   public Object value;

   /** Constructor */
   public Register() { }
   /** Constructor */
   public Register(int value) { this.value=new Integer(value); }
   /** Constructor */
   public Register(double value) { this.value=new Double(value); }
   /** Constructor */
   public Register(String value) { this.value=value; }
}