package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class InMemoryOperator implements Operator {
	ArrayList<ArrayList<Datum>> tuplesInMem;
	int index;
	int size;
	
	public InMemoryOperator(ArrayList<ArrayList<Datum>> tuplesInMem) {
		super();
		this.tuplesInMem = tuplesInMem;
		index = -1;
		size = tuplesInMem.size();
	}
	
	public int getsize() {
		return tuplesInMem.size();
	}
	
	@Override
	public void setFuncFlag() {
		// TODO Auto-generated method stub

	}
	public ArrayList<ArrayList<Datum>> getTuplesInMem(){
		return tuplesInMem;
	}
	@Override
	public ArrayList<Datum> readOneTuple() {
		// TODO Auto-generated method stub
		index++;
		if(index >= size){
			return null;
		}
		else{
			return tuplesInMem.get(index);
		}
	}

	@Override
	public ScanOperator readAllTuples(boolean isOutermost) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void reset() {
		index = -1;
		size = tuplesInMem.size();

	}

	@Override
	public void setGroupFlag() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean joinExists() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int getAllTuplesSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public List<ColumnDefinition> getColsDef() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public void close()
	{
		//do Nothing
	}

}
