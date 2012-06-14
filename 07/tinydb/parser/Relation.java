package parser;

import tinydb.Table;
import tinydb.operator.Tablescan;

public class Relation {

	public String binding;
	public Table table;
	public Tablescan tableScan;

	public Relation(String binding, Table table, Tablescan tableScan) {
		this.binding = binding;
		this.table = table;
		this.tableScan = tableScan;
	}
}
