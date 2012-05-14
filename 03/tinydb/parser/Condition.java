package parser;

import tinydb.Register;

public class Condition {
	public Register a;
	public Register b;
	public PairCondition pair;

	public Condition(Register a, Register b, PairCondition s) {
		this.a = a;
		this.b = b;
		this.pair = s;
	}
}
