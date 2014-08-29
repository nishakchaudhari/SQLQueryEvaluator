package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public interface Operator {
	public void setFuncFlag();
	public ArrayList<Datum> readOneTuple();
	public Operator readAllTuples(boolean isOutermost);
	public void reset();
	public void setGroupFlag();
	public boolean joinExists();
	public List<ColumnDefinition> getColsDef();
	public int getsize();
	public int getAllTuplesSize();
	public void close();
}
