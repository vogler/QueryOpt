package tinydb.operator;

import tinydb.*;

/** A tablescan operator */
public class Tablescan implements Operator
{
   /** The table */
   private final Table table;
   /** A small buffer */
   private final byte[] buffer=new byte[4096];
   /** Buffer pointers */
   private int bufferStart,bufferStop;
   /** The current position */
   private int filePos;
   /** Construction helper */
   private final StringBuffer buf=new StringBuffer();
   /** The io interface */
   private final java.io.RandomAccessFile io;

   /** The attributes */
   private final int[] attributes;
   /** The output */
   private final Register[] output;

   /** Constructor */
   public Tablescan(Table table)
   {
      this.table=table;
      io=table.getIOInterface();
      attributes=new int[table.getAttributeCount()];
      output=new Register[table.getAttributeCount()];
      for (int index=0;index<output.length;index++) {
	 attributes[index]=table.getAttribute(index).getType();
         output[index]=new Register();
      }
   }

   /** Open the operator */
   public void open() { bufferStart=bufferStop=0; filePos=0; }
   /** Get the next tuple */
   public boolean next() throws java.io.IOException {
      boolean escape=false;
      for (int index=0;index<attributes.length;index++) {
	 buf.setLength(0);
	 while (true) {
	    if (bufferStart>=bufferStop) {
	       io.seek(filePos);
	       int len=io.read(buffer,0,buffer.length);
	       if (len<1) return false;
	       bufferStart=0;
	       bufferStop=len;
	       filePos+=len;
	    }
	    int c=buffer[bufferStart++];
	    if (escape) { escape=false; buf.append((char)c); continue; }
	    if (c=='\r') continue;
	    if ((c==';')||(c=='\n')) {
	       Register o=output[index];
	       if (o!=null) {
		  int type=attributes[index];
		  if (type==Attribute.TYPE_INT) o.value=Integer.valueOf(buf.toString()); else
		  if (type==Attribute.TYPE_DOUBLE) o.value=Double.valueOf(buf.toString()); else
		  if (type==Attribute.TYPE_BOOL) o.value=Boolean.valueOf(buf.toString()); else
		     o.value=buf.toString();
	       }
	       break;
	    } else if (c=='\\') {
	       escape=true;
	    } else buf.append((char)c);
	 }
      }
      return true;
   }
   /** Close the operator */
   public void close() {}

   /** Get the table */
   public Table getTable() { return table; }
   /** Get all produced values */
   public Register[] getOutput() { return output; }
   /** Get one produced value */
   public Register getOutput(String name) {
      int slot=table.findAttribute(name);
      if (slot<0) return null;
      return output[slot];
   }
}
