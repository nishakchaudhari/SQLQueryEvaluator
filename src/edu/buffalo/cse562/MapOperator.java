/**
 * 
 */
package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class MapOperator implements Operator {
	
	private HashMap<Datum, ArrayList<ArrayList<Datum>>> hashMapInMem;
	private Iterator<Map.Entry<Datum, ArrayList<ArrayList<Datum>>>> iter;
	private int size;
	private ArrayList<ArrayList<Datum>> valueForReadOne = null;
	private int valueForReadOneCount = 0;
	
	public MapOperator(HashMap<Datum, ArrayList<ArrayList<Datum>>> hashmap) {
		super();
		this.hashMapInMem = hashmap;
		iter = hashMapInMem.entrySet().iterator();
		size = hashMapInMem.size();
	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#setFuncFlag()
	 */
	@Override
	public void setFuncFlag() {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#readOneTuple()
	 */
	@Override
	public ArrayList<Datum> readOneTuple() {
		ArrayList<Datum> result = null;
		if(valueForReadOne == null){
			if(iter.hasNext()){
				Map.Entry<Datum, ArrayList<ArrayList<Datum>>> entry = (Entry<Datum, ArrayList<ArrayList<Datum>>>) iter.next();
				valueForReadOne = entry.getValue();
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
		
		result = null;
		return result;
	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#readAllTuples(boolean)
	 */
	@Override
	public Operator readAllTuples(boolean isOutermost) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#reset()
	 */
	@Override
	public void reset() {
		iter = hashMapInMem.entrySet().iterator();

	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#setGroupFlag()
	 */
	@Override
	public void setGroupFlag() {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#joinExists()
	 */
	@Override
	public boolean joinExists() {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#getColsDef()
	 */
	@Override
	public List<ColumnDefinition> getColsDef() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#getsize()
	 */
	@Override
	public int getsize() {
		
		return hashMapInMem.size();
	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#getAllTuplesSize()
	 */
	@Override
	public int getAllTuplesSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#close()
	 */
	@Override
	public void close() {
		// TODO Auto-generated method stub

	}
	
	public HashMap<Datum, ArrayList<ArrayList<Datum>>> getHashMap(){
		return hashMapInMem;
	}

}
