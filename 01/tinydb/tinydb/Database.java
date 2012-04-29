package tinydb;

import java.io.*;
import java.util.*;

/** Manager for the database instance */
public class Database
{
   /// The database schema file
   private File repoFile;
   /// The base directory
   private File baseDir;
   /// All tables
   private Map  tables=new TreeMap();
   /// Unwritten changes?
   private boolean dirty;


   /** Contructor */
   private Database(String name)
   {
      repoFile=new File(name);
      baseDir=repoFile.getParentFile();
   }

   /** Open an existing database */
   public static Database open(String name)
   {
      Database db=new Database(name);
      db.read();
      return db;
   }
   /** Create a new database */
   public static Database create(String name)
   {
      Database db=new Database(name);
      db.dirty=true;
      return db;
   }
   /** Dirty? */
   private boolean isDirty() {
      if (dirty) return true;
      for (Iterator iter=tables.values().iterator();iter.hasNext();)
         if (((Table)iter.next()).isDirty())
            return true;
      return false;
   }
   /** Close the database */
   public void close()
   {
      if (isDirty())
         write();
   }
   /** Read the metadata information */
   private void read() {
      try {
	 BufferedReader in=new BufferedReader(new FileReader(repoFile));
	 while (true) {
	    String tableName=in.readLine();
	    if (tableName==null) break;

	    Table table=new Table(new File(baseDir,tableName),new File(baseDir,tableName+".idx"));
	    table.read(in);
	    tables.put(tableName,table);
	 }
	 in.close();
      } catch (java.io.IOException e) {
	 System.err.println("unable to read "+repoFile.toString());
	 e.printStackTrace(System.err);
	 System.exit(1);
      }
   }
   /** Write the metadata information */
   void write() {
      try {
	 PrintWriter out=new PrintWriter(new FileWriter(repoFile));
	 for (Iterator iter=tables.keySet().iterator();iter.hasNext();) {
	    String tableName=(String)iter.next();
	    out.println(tableName);
	    ((Table)tables.get(tableName)).write(out);
	 }
	 out.close();
	 dirty=false;
      } catch (java.io.IOException e) {
	 System.err.println("unable to write "+repoFile.toString());
	 e.printStackTrace(System.err);
	 System.exit(1);
      }
   }
   /** Create a new table */
   public Table createTable(String name)
   {
      if (tables.get(name)!=null) throw new RuntimeException("table "+name+" already exists");
      File f=new File(baseDir,name);
      f.delete();
      try { f.createNewFile(); } catch (IOException e) { throw new RuntimeException(e); }
      File i=new File(baseDir,name+".idx");
      i.delete();
      try { i.createNewFile(); } catch (IOException e) { throw new RuntimeException(e); }
      Table t=new Table(f,i);
      tables.put(name,t);
      dirty=true;
      return t;
   }
   /** Drop a table */
   public void dropTable(String name)
   {
      if (tables.get(name)==null) throw new RuntimeException("table "+name+" not found");
      File f=new File(baseDir,name);
      f.delete();
      File i=new File(baseDir,name+".idx");
      i.delete();
      tables.remove(name);
      dirty=true;
   }
   /** Get a table */
   public Table getTable(String name) { return (Table)tables.get(name); }
   /** Update the statistics */
   public void runStats() throws IOException
   {
      for (Iterator iter=tables.values().iterator();iter.hasNext();)
         ((Table)iter.next()).runStats();
   }
}
