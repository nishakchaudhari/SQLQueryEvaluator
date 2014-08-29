
package edu.buffalo.cse562;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class PushDownMapOperator implements Operator {
	
	Map<Datum, ValueContainer> index;
	private Iterator<Map.Entry<Datum, ValueContainer>> iter;
	private ArrayList<ArrayList<Datum>> valueForReadOne = null;
	private int valueForReadOneCount = 0;
	List<ColumnDefinition> cols;
	private int colsDefIndex;
	
	public PushDownMapOperator(Map<Datum, ValueContainer> index){
		this.index = index;
		iter = this.index.entrySet().iterator();
	}

	@Override
	public void setFuncFlag() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ArrayList<Datum> readOneTuple() {
		ArrayList<Datum> result = null;
		if(valueForReadOne == null){
			if(iter.hasNext()){
				Map.Entry<Datum, ValueContainer> entry = (Entry<Datum, ValueContainer>) iter.next();
				valueForReadOne = entry.getValue().tuples;
				if(valueForReadOneCount < valueForReadOne.size()){
					result = valueForReadOne.get(valueForReadOneCount);
					valueForReadOneCount++;
				}
				
				if(valueForReadOneCount == valueForReadOne.size()){
					valueForReadOne = null;
					valueForReadOneCount = 0;
				}
			}
		}
		else{
			if(valueForReadOneCount < valueForReadOne.size()){
				result = valueForReadOne.get(valueForReadOneCount);
				valueForReadOneCount++;
			}
			
			if(valueForReadOneCount == valueForReadOne.size()){
				valueForReadOne = null;
				valueForReadOneCount = 0;
			}
		}
		
		return result;
	}
	
	public Map.Entry<Datum, ValueContainer> readOneEntry(){
		if(iter.hasNext()){
			Map.Entry<Datum, ValueContainer> entry = (Entry<Datum, ValueContainer>) iter.next();
			return entry;
		}
		return null;
	}
	
	public ArrayList<ArrayList<Datum>> getValueForKey(Datum key){
		ValueContainer valcon = index.get(key);
		if(valcon == null){
			return null;
		}
		else{
			return valcon.tuples;
		}
		
	}

	@Override
	public Operator readAllTuples(boolean isOutermost) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void reset() {
		iter = index.entrySet().iterator();
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
	public List<ColumnDefinition> getColsDef() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getsize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getAllTuplesSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
