import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import tinydb.Database;
import tinydb.Register;
import tinydb.Table;
import tinydb.operator.Chi;
import tinydb.operator.CrossProduct;
import tinydb.operator.Printer;
import tinydb.operator.Projection;
import tinydb.operator.Selection;
import tinydb.operator.Tablescan;

public class Exercise1 {
	// Find all students that attended the lectures together with Schopenhauer,
	// excluding Schopenhauer himself.

	// SELECT s2.name FROM studenten s1, studenten s2, hoeren h1, hoeren h2
	// WHERE s1.name=’Schopenhauer’ AND s1.matrnr=h1.matrnr AND
	// s2.matrnr=h2.matrnr AND h1.vorlnr=h2.vorlnr AND s2.matrnr<>s1.matrnr
	public static void part1() throws IOException {
		Database db = Database.open("../data/uni");
		Table studenten = db.getTable("studenten");
		Table hoeren = db.getTable("hoeren");

		Tablescan s1 = new Tablescan(studenten);
		Tablescan s2 = new Tablescan(studenten);
		Tablescan h1 = new Tablescan(hoeren);
		Tablescan h2 = new Tablescan(hoeren);

		Register s1_name = s1.getOutput()[studenten.findAttribute("name")];
		Register s2_name = s2.getOutput()[studenten.findAttribute("name")];
		Register s1_matrnr = s1.getOutput()[studenten.findAttribute("matrnr")];
		Register s2_matrnr = s2.getOutput()[studenten.findAttribute("matrnr")];
		Register h1_matrnr = h1.getOutput()[hoeren.findAttribute("matrnr")];
		Register h2_matrnr = h2.getOutput()[hoeren.findAttribute("matrnr")];
		Register h1_vorlnr = h1.getOutput()[hoeren.findAttribute("vorlnr")];
		Register h2_vorlnr = h2.getOutput()[hoeren.findAttribute("vorlnr")];

		// s1.name=’Schopenhauer’
		Selection s1_schopenhauer = new Selection(s1, s1_name, new Register(
				"Schopenhauer"));
		// s1_schopenhauer x h1
		CrossProduct cp1 = new CrossProduct(s1_schopenhauer, h1);
		// s1.matrnr=h1.matrnr
		Selection sel1 = new Selection(cp1, s1_matrnr, h1_matrnr);
		// s2 x h2
		CrossProduct cp2 = new CrossProduct(s2, h2);
		// s2.matrnr=h2.matrnr
		Selection sel2 = new Selection(cp2, s2_matrnr, h2_matrnr);
		// sel1 x sel2
		CrossProduct cp3 = new CrossProduct(sel1, sel2);
		// h1.vorlnr=h2.vorlnr
		Selection sel3 = new Selection(cp3, h1_vorlnr, h2_vorlnr);
		// s2.matrnr<>s1.matrnr
		Chi chi = new Chi(sel3, new Chi.NotEqual(), s2_matrnr, s1_matrnr);
		Selection select = new Selection(chi, chi.getResult());

		Projection project = new Projection(select, new Register[] { s2_name });
		Printer out = new Printer(project);

		out.open();
		while (out.next())
			;
		out.close();
	}

	
	// Find all professors whose lectures attended at least two students.

	// SELECT p.name, COUNT(h.matrnr) AS hoerer FROM professoren p, vorlesungen v, hoeren h
	// WHERE p.persnr=v.gelesenvon AND v.vorlnr=h.vorlnr
	// GROUP BY p.name
	// HAVING COUNT(h.matrnr)>=2
	public static void part2a() throws IOException {
		Database db = Database.open("../data/uni");
		Table professoren = db.getTable("professoren");
		Table vorlesungen = db.getTable("vorlesungen");
		Table hoeren = db.getTable("hoeren");

		Tablescan p = new Tablescan(professoren);
		Tablescan v = new Tablescan(vorlesungen);
		Tablescan h = new Tablescan(hoeren);

		Register p_name = p.getOutput()[professoren.findAttribute("name")];
		Register p_persnr = p.getOutput()[professoren.findAttribute("persnr")];
		Register v_gelesenvon = v.getOutput()[vorlesungen.findAttribute("gelesenvon")];
		Register v_vorlnr = v.getOutput()[vorlesungen.findAttribute("vorlnr")];
		Register h_vorlnr = h.getOutput()[hoeren.findAttribute("vorlnr")];
		Register h_matrnr = h.getOutput()[hoeren.findAttribute("matrnr")];

		// p x v
		CrossProduct cp1 = new CrossProduct(p, v);
		// p.persnr=v.gelesenvon
		Selection sel1 = new Selection(cp1, p_persnr, v_gelesenvon);
		// sel1 x h
		CrossProduct cp2 = new CrossProduct(sel1, h);
		// v.vorlnr=h.vorlnr
		Selection sel2 = new Selection(cp2, v_vorlnr, h_vorlnr);
		
		Projection project = new Projection(sel2, new Register[] { p_name, h_matrnr });
		
		Printer out = new Printer(project);
		out.open();
		Map<String, Set<String>> map = new HashMap<String, Set<String>>();
		System.out.println("== all values");
		System.out.println("p.name matrnr");
		while (out.next()){
			Register[] row = out.getOutput();
			String prof = row[0].value.toString();
			String matrnr = row[1].value.toString();
			if(!map.containsKey(prof)){
				map.put(prof, new HashSet<String>());
			}
			map.get(prof).add(matrnr);
			
		}
		out.close();
		System.out.println("== having count(h.matrnr)>=2");
		System.out.println("p.name count");
		for(Entry<String, Set<String>> e : map.entrySet()){
			if(e.getValue().size() >= 2){
				System.out.println(e.getKey()+" "+e.getValue().size());
			}
		}
	}
	
	
	// Find all professors whose lectures attended at least two students.

	// SELECT p.name FROM professoren p, vorlesungen v, hoeren h1, hoeren h2
	// WHERE p.persnr=v.gelesenvon AND v.vorlnr=h1.vorlnr AND v.vorlnr=h2.vorlnr
	// AND h1.matrnr<>h2.matrnr
	public static void part2b() throws IOException {
		Database db = Database.open("../data/uni");
		Table professoren = db.getTable("professoren");
		Table vorlesungen = db.getTable("vorlesungen");
		Table hoeren = db.getTable("hoeren");

		Tablescan p = new Tablescan(professoren);
		Tablescan v = new Tablescan(vorlesungen);
		Tablescan h1 = new Tablescan(hoeren);
		Tablescan h2 = new Tablescan(hoeren);

		Register p_name = p.getOutput()[professoren.findAttribute("name")];
		Register p_persnr = p.getOutput()[professoren.findAttribute("persnr")];
		Register v_gelesenvon = v.getOutput()[vorlesungen.findAttribute("gelesenvon")];
		Register v_vorlnr = v.getOutput()[vorlesungen.findAttribute("vorlnr")];
		Register h1_vorlnr = h1.getOutput()[hoeren.findAttribute("vorlnr")];
		Register h2_vorlnr = h2.getOutput()[hoeren.findAttribute("vorlnr")];
		Register h1_matrnr = h1.getOutput()[hoeren.findAttribute("matrnr")];
		Register h2_matrnr = h2.getOutput()[hoeren.findAttribute("matrnr")];

		// p x v
		CrossProduct cp1 = new CrossProduct(p, v);
		// p.persnr=v.gelesenvon
		Selection sel1 = new Selection(cp1, p_persnr, v_gelesenvon);
		// sel1 x h
		CrossProduct cp2 = new CrossProduct(sel1, h1);
		// v.vorlnr=h1.vorlnr
		Selection sel2 = new Selection(cp2, v_vorlnr, h1_vorlnr);
		// sel2 x h2
		CrossProduct cp3 = new CrossProduct(sel2, h2);
		// v.vorlnr=h2.vorlnr
		Selection sel3 = new Selection(cp3, v_vorlnr, h2_vorlnr);
		// h1.matrnr<>h2.matrnr
		Chi chi = new Chi(sel3, new Chi.NotEqual(), h1_matrnr, h2_matrnr);
		Selection sel4 = new Selection(chi, chi.getResult());
		
		Projection project = new Projection(sel4, new Register[] { p_name });
		Printer out = new Printer(project);

		out.open();
		while (out.next())
			;
		out.close();
	}

	public static void main(String[] args) throws java.io.IOException {
		System.out.println("=== Part 1 ===");
		System.out
				.println("Find all students that attended the lectures together with Schopenhauer, excluding Schopenhauer himself.");
		part1();
		System.out.println();
		System.out.println("=== Part 2 (using HAVING) ===");
		System.out
				.println("Find all professors whose lectures attended at least two students.");
		part2a();
		System.out.println();
		System.out.println("=== Part 2 (using 2x hoeren) ===");
		System.out
		.println("Find all professors whose lectures attended at least two students.");
		part2b();
	}
}
