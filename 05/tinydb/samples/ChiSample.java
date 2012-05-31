import tinydb.*;
import tinydb.operator.*;

public class ChiSample
{
   public static void main(String[] args) throws java.io.IOException
   {
      Database db=Database.open("../data/uni");
      Table studenten=db.getTable("studenten");

      Tablescan scanStudenten=new Tablescan(studenten);

      Register name=scanStudenten.getOutput()[studenten.findAttribute("name")];
      Register semester=scanStudenten.getOutput()[studenten.findAttribute("semester")];
      
      // find all students where semester num. is not 2
      Register two=new Register(2);
      Chi chi=new Chi(scanStudenten,new Chi.NotEqual(),semester,two);
      Selection select=new Selection(chi,chi.getResult());
      Projection project=new Projection(select,new Register[]{name});
      Printer out=new Printer(project);

      out.open();
      while (out.next());
      out.close();
   }
}
