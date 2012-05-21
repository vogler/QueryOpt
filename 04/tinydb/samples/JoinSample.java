import tinydb.*;
import tinydb.operator.*;

public class JoinSample
{
   public static void main(String[] args) throws java.io.IOException
   {
      Database db=Database.open("../data/uni");
      Table professoren=db.getTable("professoren");
      Table vorlesungen=db.getTable("vorlesungen");

      Tablescan scanProfessoren=new Tablescan(professoren);
      Tablescan scanVorlesungen=new Tablescan(vorlesungen);

      Register name=scanProfessoren.getOutput()[professoren.findAttribute("name")];
      Register persnr=scanProfessoren.getOutput()[professoren.findAttribute("persnr")];
      Register titel=scanVorlesungen.getOutput()[vorlesungen.findAttribute("titel")];
      Register gelesenvon=scanVorlesungen.getOutput()[vorlesungen.findAttribute("gelesenvon")];

      CrossProduct cp=new CrossProduct(scanProfessoren,scanVorlesungen);
      Selection select=new Selection(cp,persnr,gelesenvon);
      Projection project=new Projection(select,new Register[]{name,titel});
      Printer out=new Printer(project);

      out.open();
      while (out.next());
      out.close();
   }
}
