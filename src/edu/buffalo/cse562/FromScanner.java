package edu.buffalo.cse562;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;


public class FromScanner implements FromItemVisitor {
	File basePath;
	File swapDir;
	File indexPath;
	HashMap<String,CreateTable> tables;
	List selectColList; 
	public static HashMap<String,String> tableNameMapping;
	
	public ArrayList<Column[]> schema =null;
	public ArrayList<Operator> source = null;

	public FromScanner(File basePath,HashMap<String,CreateTable> tables, List selectlist, File swapDir,File indexPath){
		this.basePath = basePath;
		this.tables = tables;
		source = new ArrayList<Operator>();
		schema = new ArrayList<Column []>();
		this.selectColList = selectlist;		
		this.swapDir = swapDir;
		tableNameMapping = new HashMap<String,String>();
		this.indexPath = indexPath;
	}
	public void visit(SubJoin subJoin){

	}
	public void visit(SubSelect subselect){
		SelectBody subSelectbody = subselect.getSelectBody();

		SelectEval selectEval = new SelectEval(basePath, tables,swapDir,indexPath);

		subSelectbody.accept(selectEval);
		//UtilQE.addOperToIndexOperMap(subselect.getAlias(), source.size());
		source.add(selectEval.oper);

		List subColList = selectEval.selectColList;

		Column [] tempSchema =null;
		tempSchema = new Column[subColList.size()];
		Table t = new Table(subselect.getAlias(), subselect.getAlias());
		for(int i = 0; i < subColList.size(); i++){
			if(((SelectExpressionItem)subColList.get(i)).getAlias() != null){
				tempSchema[i] = new Column(t, ((SelectExpressionItem)subColList.get(i)).getAlias());
			}
			else{
				tempSchema[i] = new Column(t, subColList.get(i).toString());
			}			
		}
		schema.add(tempSchema);		
	}
	
	public void visit(Table tableName){
		String originalName = tableName.getName().toUpperCase();
		Operator tempSource = null;
		Column [] tempSchema =null;
		if(tableName.getAlias() != null){
			tableNameMapping.put(tableName.getAlias(),originalName);
			tableName.setName(tableName.getAlias());
		}
		CreateTable table = tables.get(originalName);
		List cols = table.getColumnDefinitions();
		tempSchema = new Column[cols.size()];
		for (int i=0;i<cols.size();i++){
			ColumnDefinition col = (ColumnDefinition)cols.get(i);
			tempSchema[i]=new Column(tableName,col.getColumnName());	
		}
		schema.add(tempSchema);
		File file = null;
		if(tableName.getName().equalsIgnoreCase("LINEITEM")){
			file = new File(indexPath, originalName + ".dbr.s");
		}else if(tableName.getName().equalsIgnoreCase("ORDERS")){
			file = new File(indexPath, originalName + ".dbr.s");
		}
		else{
			file = new File(basePath, originalName + ".dat");
		} 
		if(file.exists()){
			tempSource = new ScanOperator(file, cols,originalName);
			//UtilQE.addOperToIndexOperMap(originalName, source.size());
			source.add(tempSource); 
		}
		else{
			System.out.println("File - " + originalName + ".dat " + "does not exist");
		}
		
		/*String[] indexColNames = UtilQE.getColumnsForTableName(originalName);
		for(String indexColName : indexColNames){
			String indexFileName =  originalName + "_" + indexColName;
			int colDefIndex = -1;
			for(int i = 0; i < tempSchema.length; i++){
				if(tempSchema[i].getColumnName().equalsIgnoreCase(indexColName)){
					colDefIndex = i;
					break;
				}
			}
			tempSource = new IndexOperator(UtilQE.getIndexPath(), indexFileName, cols, colDefIndex);
			UtilQE.addOperToIndexOperMap(indexFileName, source.size());
			source.add(tempSource); 
		}*/
		
		
	}
}