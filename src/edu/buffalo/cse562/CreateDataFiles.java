package edu.buffalo.cse562;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.Index;

import jdbm.PrimaryHashMap;
import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import edu.buffalo.cse562.Datum.DatumLong;

public class CreateDataFiles {
	String tableName;
	Operator scanOperator = null;
	HashSet<String> index = null;
	ArrayList<String> columns = null;
	HashSet<Integer> indexColumns = null;
	File basePath = null;
	File dataPath = null;

	List<ColumnDefinition> cols = null;

	public CreateDataFiles(String tableName, List<ColumnDefinition> cols,File indexDir,File dataDir) {
		columns = new ArrayList<String>();
		for(int i=0;i<cols.size();i++){
			columns.add(cols.get(i).getColumnName().toString());
		}
		this.tableName = tableName;
		this.cols = cols;
		basePath = indexDir;
		dataPath = dataDir;
		createScanOperator(cols);		
	}
	private void createScanOperator(List<ColumnDefinition> cols){
		Operator tempSource = null;
		String originalName = tableName;
		File file = new File(dataPath, originalName + ".dat");
		if(file.exists()){
			scanOperator = new ScanOperator(file, cols,null);			 
		}else{
			System.out.println("File - " + originalName + ".dat " + "does not exist");
		}		
	}
	public void createSingleIndex(Integer indexColumn){
		String column = cols.get(indexColumn).getColumnName();
		String fileName = basePath + "/"+ tableName + "_" + column;
		RecordManager recMan = null;
		try {
			recMan = RecordManagerFactory.createRecordManager(fileName);
			String recordName = tableName + "_" + column;	
			PrimaryTreeMap<Datum, ValueContainer> index = recMan.treeMap(recordName,new DatumSerializer(cols),new KeySerializer(cols.get(indexColumn)));
			//PrimaryTreeMap<Datum, ArrayList<ArrayList<Datum>>> index = recMan.treeMap(recordName);
			ArrayList<ArrayList<Datum>> rows = null;
			ArrayList<Datum> row;
			scanOperator.reset();
			row = scanOperator.readOneTuple();
			while(row != null){
				Datum key = row.get(indexColumn);
				if(index.containsKey(key)){
					index.get(key).tuples.add(row);
					index.get(key).count++;
				}else{	
					rows = new ArrayList<ArrayList<Datum>>();
					rows.add(row);
					ValueContainer value = new ValueContainer(rows);
					value.count = 1;
					index.put(key, value);
				}
				row = scanOperator.readOneTuple();				
			}
			recMan.commit();
			recMan.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		scanOperator.reset();
	}
	public void readIndex(){
		RecordManager recMan = null;	
		String fileName = basePath +"/"+ tableName + "_" + "partkey";		
		try {
			recMan = RecordManagerFactory.createRecordManager(fileName);
			String recordName = tableName + "_" + "partkey";			
			PrimaryTreeMap<Datum,ValueContainer> index = recMan.treeMap(recordName,new DatumSerializer(cols),	new KeySerializer(cols.get(0)));	
			//	PrimaryTreeMap<Datum,ArrayList<ArrayList<Datum>>> index = recMan.treeMap(recordName);
			DatumLong d = new DatumLong(1);
			ArrayList<ArrayList<Datum>> rows = index.get(d).tuples;
			recMan.close();	
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void createIndices(){		
		Iterator<Integer> iter =indexColumns.iterator();
		Iterator<String> iter1 =index.iterator();
		while (iter.hasNext()) {
			createSingleIndex(iter.next());			
		}
	}
	public void allIndices(ArrayList<Index> index2){
		ArrayList<String> tempIndex = null;
		if(index2 != null){
			tempIndex = new ArrayList<String>();
			for(int i=0;i< index2.size();i++){
				for(int j =0;j<index2.get(i).getColumnsNames().size();j++){
					tempIndex.add(index2.get(i).getColumnsNames().get(j).toString());					
				}
			}		
		}
		indexMapping(tempIndex);
		writeToPropertyFile();
	}
	public void indexMapping(ArrayList<String> tempIndex){
		Map<String,Integer> cols = new HashMap<String,Integer>();
		index = new HashSet<String>();
		indexColumns = new HashSet<Integer>();
		for(int i=0;i<columns.size();i++){
			cols.put(columns.get(i), i);
		}
		for(int i=0;i<tempIndex.size();i++){			
			index.add(tempIndex.get(i));
			indexColumns.add(cols.get(tempIndex.get(i)));
		}
	}
	private void writeToPropertyFile(){	
		Properties prop = null;
		OutputStream output = null;

		prop = new Properties();
		output = null;
		try {
			output = new FileOutputStream("config.properties",true);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String cols =""; 
		Iterator<String> iter1 =index.iterator();
		while (iter1.hasNext()) {
			cols += iter1.next() + ",";
		}
		// set the properties value
		prop.setProperty(tableName.toUpperCase(), cols);
		try{
			prop.store(output, null);
		}catch (IOException io) {
			io.printStackTrace();
		}finally {
			if (output != null) {
				try {
					output.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}	
}