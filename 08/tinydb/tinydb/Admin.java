package tinydb;

/** Administration commands */
public class Admin
{
   /** Create a new database */
   public static void initDB(String db) {
      Database.create(db).close();
      System.out.println("ok");
   }
   /** Create a new table */
   public static void createTable(String dbName,String tableName,String[] rest)
   {
      Database db=Database.open(dbName);
      Table table=db.createTable(tableName);
      for (int index=0;index<rest.length;index++) {
	 String name=rest[index++];
	 boolean key=false;
	 if ((index<rest.length)&&(rest[index].equals("key"))) { key=true; index++; }
	 if (index>=rest.length) {
	    System.err.println("invalid attribute specification");
	    return;
	 }
	 int type;
	 if (rest[index].equals("int")) type=Attribute.TYPE_INT; else
	 if (rest[index].equals("double")) type=Attribute.TYPE_DOUBLE; else
	 if (rest[index].equals("bool")) type=Attribute.TYPE_BOOL; else
	 if (rest[index].equals("string")) type=Attribute.TYPE_STRING; else
	    { System.err.println("invalid attribute type "+rest[index]); return; }
	 table.addAttribute(name,type,key);
      }
      db.close();
      System.out.println("ok");
   }
   /** Remove a table */
   public static void dropTable(String dbName,String tableName)
   {
      Database db=Database.open(dbName);
      db.dropTable(tableName);
      db.close();
      System.out.println("ok");
   }
   /** Insert values into a database */
   public static void insertValues(String dbName,String tableName,String[] rest)
   {
      Database db=Database.open(dbName);
      Table table=db.getTable(tableName);
      if (table==null) { System.err.println("unknown table "+tableName); return; }
      if (rest.length!=table.getAttributeCount()) {
         System.err.println("the table has "+table.getAttributeCount()+" columns, "+rest.length+" values were provided");
         return;
      }
      Object[] values=new Object[rest.length];
      for (int index=0;index<rest.length;index++) {
	 Attribute a=table.getAttribute(index);
	 if (a.getType()==Attribute.TYPE_INT) values[index]=Integer.valueOf(rest[index]); else
	 if (a.getType()==Attribute.TYPE_DOUBLE) values[index]=Double.valueOf(rest[index]); else
	 if (a.getType()==Attribute.TYPE_BOOL) values[index]=Boolean.valueOf(rest[index]); else
            values[index]=rest[index];
      }
      try {
	 table.insertValues(values);
      } catch (java.io.IOException e) { throw new RuntimeException(e); }
      db.close();
      System.out.println("ok");
   }
   /** Bulkload */
   public static void bulkload(String dbName,String tableName,String[] rest)
   {
      Database db=Database.open(dbName);
      Table table=db.getTable(tableName);
      if (table==null) { System.err.println("unknown table "+tableName); return; }
      if (rest.length!=1) {
         System.err.println("source file expected");
         return;
      }
      try {
	 java.io.BufferedReader in=new java.io.BufferedReader(new java.io.FileReader(rest[0]));
	 Object[] values=new Object[table.getAttributeCount()];
	 while (true) {
	    String line=in.readLine();
	    if (line==null) break;
	    char[] data=line.toCharArray();
	    int last=0,writer=0;
	    for (int index=0;index<data.length;index++)
	       if (data[index]=='|') {
		  String d=new String(data,last,index-last);
		  Attribute a=table.getAttribute(writer);
		  if (a.getType()==Attribute.TYPE_INT) values[writer]=Integer.valueOf(d); else
		  if (a.getType()==Attribute.TYPE_DOUBLE) values[writer]=Double.valueOf(d); else
		  if (a.getType()==Attribute.TYPE_BOOL) values[writer]=Boolean.valueOf(d); else
		     values[writer]=d;
		  writer++; last=index+1;
	       }
   	    table.insertValues(values);
	 }
      } catch (java.io.IOException e) { throw new RuntimeException(e); }
      db.close();
      System.out.println("ok");
   }
   /** Show the content of a table */
   public static void dumpTable(String dbName,String tableName)
   {
      Database db=Database.open(dbName);
      Table table=db.getTable(tableName);
      if (table==null) { System.err.println("unknown table "+tableName); return; }
      tinydb.operator.Printer p=new tinydb.operator.Printer(new tinydb.operator.Tablescan(table));
      try {
	 p.open();
	 while (p.next());
	 p.close();
      } catch (java.io.IOException e) { throw new RuntimeException(e); }
      db.close();
      System.out.println("ok");
   }
   /** Update the statistics */
   public static void runStats(String dbName) {
      Database db=Database.open(dbName);
      try { db.runStats(); } catch (java.io.IOException e) { throw new RuntimeException(e); }
      db.close();
      System.out.println("ok");
   }
   /** Display a short help */
   private static void showHelp()
   {
      System.err.println("usage: java tinydb.Admin [cmd] [db] [arg(s)]");
      System.err.println("known commands:");
      System.err.println("initdb [db] - creates a new database");
      System.err.println("createtable [db] [table] [attributes] - creates a new table");
      System.err.println("droptable [db] [table] - deletes a table");
      System.err.println("insertvalues [db] [table] [values] - insert values into a table");
      System.err.println("dumptable [db] [table] - show the content of a table");
      System.err.println("runstats [db] - update the statistics");
   }
   /** Entry point */
   public static void main(String[] args)
   {
      if (args.length<2) { showHelp(); return; }
      String cmd=args[0];
      String db=args[1];
      String table;
      String[] rest;
      if (args.length>2) {
	 table=args[2];
         rest=new String[args.length-3];
         System.arraycopy(args,3,rest,0,rest.length);
      } else {
	 table=null;
	 rest=null;
      }
      if (cmd.equals("initdb")) initDB(db); else
      if (cmd.equals("runstats")) {
	 runStats(db);
      } else {
	 if (table==null) { System.err.println("no table specified!"); showHelp(); return; }
	 if (cmd.equals("createtable")) createTable(db,table,rest); else
         if (cmd.equals("droptable")) dropTable(db,table); else
         if (cmd.equals("insertvalues")) insertValues(db,table,rest); else
         if (cmd.equals("bulkload")) bulkload(db,table,rest); else
         if (cmd.equals("dumptable")) dumpTable(db,table); else
            showHelp();
      }
   }
}
