package tinydb;

import java.io.*;
import java.util.*;

/** A database table */
public class Table
{
   /** The file */
   File file;
   /** The index file */
   File indexFile;
   /** The cardinality */
   int cardinality;
   /** The attributes */
   Attribute[] attributes=new Attribute[0];
   /** The indices */
   TreeMap[] indices=new TreeMap[0];
   /** The io interface */
   private RandomAccessFile io;
   /** Unwritten changes? */
   private boolean dirty;

   /** Constructor */
   Table(File file,File indexFile)
   {
      this.file=file;
      this.indexFile=indexFile;
   }
   /** Access interface for the scans */
   public RandomAccessFile getIOInterface() { return io; }
   /** Index interface for the scans */
   public TreeMap getIndexInterface(int attribute) { return indices[attribute]; }

   /** Dirty? */
   public boolean isDirty() {
      return dirty;
   }

   /** Add an attribute */
   void addAttribute(String name,int type,boolean key)
   {
      Attribute a=new Attribute();
      a.name=name;
      a.type=type;
      a.key=key;

      Attribute[] newAttributes=new Attribute[attributes.length+1];
      System.arraycopy(attributes,0,newAttributes,0,attributes.length);
      newAttributes[attributes.length]=a;
      attributes=newAttributes;

      TreeMap[] newIndices=new TreeMap[indices.length+1];
      System.arraycopy(indices,0,newIndices,0,indices.length);
      if (key) newIndices[indices.length]=new TreeMap();
      indices=newIndices;

      dirty=true;
   }

   /** Read */
   void read(BufferedReader in) throws IOException
   {
      List list=new LinkedList();
      cardinality=Integer.parseInt(in.readLine());
      while (true) {
	 String line=in.readLine();
	 if (line==null) break;
	 if (line.equals("")) break;
	 Attribute a=new Attribute();
	 a.read(line);
	 list.add(a);
      }
      attributes=(Attribute[])list.toArray(attributes);

      indices=new TreeMap[attributes.length];
      ObjectInputStream indicesIn=new ObjectInputStream(new BufferedInputStream(new FileInputStream(indexFile)));
      try {
	 for (int index=0;index<attributes.length;index++)
	    if (attributes[index].key||attributes[index].index)
	       indices[index]=(TreeMap)indicesIn.readObject();
      } catch (ClassNotFoundException e) {
	 throw new RuntimeException(e);
      }
      indicesIn.close();

      io=new RandomAccessFile(file,"rw");
   }
   /** Write */
   void write(PrintWriter out) throws IOException
   {
      out.println(cardinality);
      for (int index=0;index<attributes.length;index++)
         attributes[index].write(out);
      out.println("");

      ObjectOutputStream indicesOut=new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(indexFile)));
      for (int index=0;index<attributes.length;index++)
         if (attributes[index].key||attributes[index].index)
            indicesOut.writeObject(indices[index]);
      indicesOut.close();

      dirty=false;
   }
   /** Insert a new tuple */
   void insertValues(Object[] values) throws IOException
   {
      // Check key constraints first
      for (int index=0;index<attributes.length;index++)
         if (attributes[index].key)
            if (indices[index].get(values[index])!=null)
               throw new RuntimeException("key constraint for "+attributes[index].name+" violated");
      // Seek to the end of file and write the new tuple
      int pos=(int)io.length();
      io.seek(pos);
      for (int index=0;index<attributes.length;index++) {
	 if (index>0) io.write(';');
	 String value=values[index].toString();
	 int len=value.length();
	 for (int index2=0;index2<len;index2++) {
	    char c=value.charAt(index2);
	    if ((c==';')||(c=='\\'))
	       { io.write('\\'); io.write(c); } else
	       io.write(c);
	 }
      }
      io.write('\n');
      // Update the indices
      Integer i=new Integer(pos);
      for (int index=0;index<attributes.length;index++)
         if (attributes[index].key) {
            indices[index].put(values[index],i);
	 } else if (attributes[index].index) {
	    ArrayList l=(ArrayList)indices[index].get(values[index]);
	    if (l==null) {
	       l=new ArrayList();
	       indices[index].put(values[index],l);
	    }
	    l.add(i);
	 }
      cardinality++;
      dirty=true;
   }
   /** Update the statistics */
   void runStats() throws IOException {
      // Should use a group by operator instead...
      TreeSet[] stats=new TreeSet[attributes.length];
      for (int index=0;index<stats.length;index++)
         stats[index]=new TreeSet();
      // Perform the scan
      tinydb.operator.Tablescan scan=new tinydb.operator.Tablescan(this);
      Register[] output=scan.getOutput();
      scan.open();
      while (scan.next()) {
	 for (int index=0;index<stats.length;index++)
	    stats[index].add(output[index].value);
      }
      scan.close();
      // Update the statistics
      for (int index=0;index<attributes.length;index++) {
	 attributes[index].uniqueValues=stats[index].size();
	 switch (attributes[index].type) {
	    case Attribute.TYPE_INT: attributes[index].size=4; break;
	    case Attribute.TYPE_DOUBLE: attributes[index].size=8; break;
	    case Attribute.TYPE_BOOL: attributes[index].size=1; break;
	    case Attribute.TYPE_STRING:
	       attributes[index].size=0;
	       for (Iterator iter=stats[index].iterator();iter.hasNext();)
	          attributes[index].size+=((String)iter.next()).length();
	       if (stats[index].size()>0) attributes[index].size/=stats[index].size();
	       break;
	 }
	 if (stats[index].size()>0) {
	    attributes[index].minValue=stats[index].first();
	    attributes[index].maxValue=stats[index].last();
	 }
      }
      dirty=true;
   }

   /** The cardinality */
   public int getCardinality() { return cardinality; }
   /** The number of attributes */
   public int getAttributeCount() { return attributes.length; }
   /** A specific attribute */
   public Attribute getAttribute(int index) { return attributes[index]; }
   /** Search a specific attriubt */
   public int findAttribute(String name) {
      for (int index=0;index<attributes.length;index++)
         if (attributes[index].getName().equals(name))
            return index;
      return -1;
   }
}
