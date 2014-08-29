package edu.buffalo.cse562;

import java.io.*;
import java.util.*;

import edu.buffalo.cse562.Datum.DatumDate;

import net.sf.jsqlparser.statement.create.table.*;

public class ScanOperator implements Operator {
	BufferedReader input;
	List<ColumnDefinition> colsDef;
	File f;
	boolean isFunc=false;
	boolean isGroup = false;
	int allTuplesSize = 0;
	char[] datumType = null;  //long-l : float-f : string-s : date-d
	boolean isSizingDone = false;
	DatumDate startDate = null;
	DatumDate endDate = null;
	String tableName = null;
	int count ;

	public ScanOperator(File f, List colsDef,String tableName){
		this.colsDef = colsDef;
		this.f = f;
		reset();
		datumType = new char[colsDef.size()];
		calcSize();
		isSizingDone = false;
		this.tableName = tableName;
	}

	public void close(){
		try {
			input.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void calcSize(){
		if(colsDef == null) return; 
		for(int i = 0; i< colsDef.size(); i++){
			if(colsDef.get(i).getColDataType().getDataType().equalsIgnoreCase("long")  || 
					colsDef.get(i).getColDataType().getDataType().equalsIgnoreCase("number") ||
					colsDef.get(i).getColDataType().getDataType().equalsIgnoreCase("int") ||
					colsDef.get(i).getColDataType().getDataType().equalsIgnoreCase("integer") ){
				allTuplesSize += 4;
				datumType[i] = 'l';
			}
			else if(colsDef.get(i).getColDataType().getDataType().equalsIgnoreCase("double") ||
					colsDef.get(i).getColDataType().getDataType().equalsIgnoreCase("decimal") ||
					colsDef.get(i).getColDataType().getDataType().equalsIgnoreCase("float")){
				allTuplesSize += 8;
				datumType[i] = 'f';
			}
			else if(colsDef.get(i).getColDataType().getDataType().equalsIgnoreCase("date")){
				allTuplesSize += 11;
				datumType[i] = 'd';
			}
			else{
				allTuplesSize += 8;
				datumType[i] = 's';
			}
		}
	}


	public int getsize() {
		return (int)f.length();	
	}

	public List<ColumnDefinition> getColsDef(){
		return this.colsDef;
	}

	public void reset() {

		try{
			input =new BufferedReader(new FileReader(f));
		}catch(IOException e){
			e.printStackTrace();
			input = null;
		}

	}
	public ArrayList<Datum> readOneTuple(){
		String line = null;
		if(input == null){
			return null;
		}
		try{
			line = input.readLine();
		}catch(IOException e){
			e.printStackTrace();
		}
		if (line == null)
			return null;
		String []cols = line.split("\\|");
		ArrayList<Datum> ret = new ArrayList<Datum>();

		for(int i = 0; i< cols.length; i++){
			if(datumType[i] == 'l'){
				ret.add(new Datum.DatumLong(cols[i]));
				//allTuplesSize += 4;
			}
			else if(datumType[i] == 'f'){
				ret.add(new Datum.DatumDouble(cols[i]));
			}
			else if(datumType[i] == 'd'){
				ret.add(new Datum.DatumDate(cols[i]));
			}
			else{
				ret.add(new Datum.DatumString(cols[i]));
				if(isSizingDone == false){
					isSizingDone = true;
					allTuplesSize -= 8;
					allTuplesSize += cols[i].length();
				}
			}
		}
		if(tableName!= null){
			if(tableName.equals("LINEITEM")){
				if(startDate!= null && endDate != null){
					DatumDate  temp = (DatumDate) ret.get(12);
					if(endDate.minorThan(temp)|| endDate.equals(temp)){
					//	ret = null;
					}						
				}
			}
			if(tableName.equals("ORDERS")){
				if(startDate!= null && endDate != null){
					DatumDate  temp = (DatumDate) ret.get(4);
					if(endDate.minorThan(temp)|| endDate.equals(temp)){
						ret = null;
					}
				}
			}
		}
		count++;
		return ret;
	}


	@Override
	public void setFuncFlag() {
		this.isFunc=true;
	}
	public void setStartEndDate(DatumDate start,DatumDate end){
		startDate = start;
		endDate = end;
	}

	@Override
	public ScanOperator readAllTuples(boolean isoutermost)
	{		
		return null;
	}
	@Override
	public void setGroupFlag() {
		this.isGroup=true;
	}
	@Override
	public boolean joinExists() {
		return false;
	}

	@Override
	public int getAllTuplesSize() {
		return allTuplesSize;
	}

	public String getFileName(){
		return this.f.getName();
	}

	public void closeScanOper(){
		try {
			this.input.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}