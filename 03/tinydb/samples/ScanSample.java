import tinydb.*;
import tinydb.operator.*;

public class ScanSample
{
   public static void main(String[] args) throws java.io.IOException
   {
      Database db=Database.open("../data/uni");
      Table studenten=db.getTable("studenten");

      Tablescan scan=new Tablescan(studenten);
      Register name=scan.getOutput()[studenten.findAttribute("name")];

      scan.open();
      while (scan.next())
	 System.out.println(name.value);
      scan.close();
   }
}