import tinydb.*;
import tinydb.operator.*;

public class SelectSample
{
   public static void main(String[] args) throws java.io.IOException
   {
      Database db=Database.open("../data/uni");
      Table studenten=db.getTable("studenten");

      Tablescan scan=new Tablescan(studenten);
      Register name=scan.getOutput()[studenten.findAttribute("name")];
      Register semester=scan.getOutput()[studenten.findAttribute("semester")];
      Register two=new Register(2);
      Selection select=new Selection(scan,semester,two);

      select.open();
      while (select.next())
	 System.out.println(name.value);
      select.close();
   }
}