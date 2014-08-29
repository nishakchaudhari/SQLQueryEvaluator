package edu.buffalo.cse562;

import java.util.ArrayList;

public class ValueContainer {
	ArrayList<ArrayList<Datum>> tuples;
	int count=1;
	public ValueContainer(ArrayList<ArrayList<Datum>> tuples){
		this.tuples = tuples;
	}
}
