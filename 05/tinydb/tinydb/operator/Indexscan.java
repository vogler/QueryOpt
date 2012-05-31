package tinydb.operator;

import java.util.TreeMap;
import java.util.Iterator;
import tinydb.*;

/** A indexscan operator */
public class Indexscan implements Operator
{
   /** The table */
   private final Table table;
   /** The index */
   private final TreeMap index;
   /** The iterator over the index */
   private Iterator iter;
   /** The bounds */
   private final Register lowerBounds,upperBounds;
   /** A small buffer */
   private final byte[] buffer=new byte[4096];
   /** Buffer pointers */
   private int bufferStart,bufferStop;
   /** Construction helper */
   private final StringBuffer buf=new StringBuffer();
   /** The io interface */
   private final java.io.RandomAccessFile io;

   /** The attributes */
   private final int[] attributes;
   /** The output */
   private final Register[] output;

   /** Constructor */
   public Indexscan(Table table,int indexAttribute,Register lowerBounds,Register upperBounds)
   {
      this.table=table;
      this.index=table.getIndexInterface(indexAttribute);
      this.lowerBounds=lowerBounds;
      this.upperBounds=upperBounds;
      io=table.getIOInterface();
      attributes=new int[table.getAttributeCount()];
      output=new Register[table.getAttributeCount()];
      for (int index=0;index<output.length;index++) {
	 attributes[index]=table.getAttribute(index).getType();
         output[index]=new Register();
      }
   }

   /** Open the operator */
   public void open() {
      bufferStart=bufferStop=0;
      if (lowerBounds!=null)
         iter=index.tailMap(lowerBounds.value).entrySet().iterator(); else
         iter=index.entrySet().iterator();
   }
   /** Get the next tuple */
   public boolean next() throws java.io.IOException {
      // Check the iterator
      if (!iter.hasNext())
         return false;
      // Check the next entry
      java.util.Map.Entry e=(java.util.Map.Entry)iter.next();
      Object key=e.getKey();
      int filePos=((Integer)e.getValue()).intValue();
      if (upperBounds!=null) {
	 if (((Comparable)key).compareTo(upperBounds.value)>0)
	    return false;
      }
      // Read the tuple
      bufferStart=bufferStop=0;
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

   /** Get all produced values */
   public Register[] getOutput() { return output; }
}
