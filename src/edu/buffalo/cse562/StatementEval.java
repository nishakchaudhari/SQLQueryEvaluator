package edu.buffalo.cse562;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

public class StatementEval implements StatementVisitor {

	File dataDir;
	File swapDir;
	File indexDir;
	static HashMap<String, CreateTable> tables=new HashMap<String, CreateTable>();
	static SplitFiles split = new SplitFiles();
	ArrayList<ArrayList<Index>> indices=new ArrayList<ArrayList<Index>>();
	Boolean buildFlag;
	public StatementEval(File dataDir, File swapDir,boolean buildFlag,File indexDir) {
		this.dataDir=dataDir;
		this.swapDir = swapDir;
		this.buildFlag = buildFlag;
		this.indexDir = indexDir;
	}
	public StatementEval() {
		// TODO Auto-generated constructor stub
	}
	public void dump(Operator oper){
		//Datum[] row = oper.readOneTuple();
		ArrayList<ArrayList<Datum>> recordsInMem =  null;

		Operator outputOper = oper.readAllTuples(true);
		//recordsInMem = oper.readAllTuples(true);
		//			Iterator<Datum> it = null;
		//			for (ArrayList<Datum> row: recordsInMem)
		//			{
		//				if(row != null){
		//					it=row.iterator();
		//					//while(row != null){	
		//					for(;it.hasNext();)
		//					{
		//						System.out.print(it.next().toString());
		//						if(it.hasNext()){
		//							System.out.print("|");
		//							
		//						}
		//					}
		//					System.out.println("");
		//				}
		//				
		//				//row = oper.readOneTuple();			
		//				//}
		/*//			}
			long endTime = System.currentTimeMillis();
			long diff = endTime - startTime;
			//System.out.println("ends");
			PrintWriter pw = new PrintWriter(System.out);
			pw.print("Time taken: " + diff);*/

	}
	@Override
	public void visit(Select select) {
		if(!buildFlag){
			UtilQE.createIndexInfo(tables, indexDir.toString());
			SelectBody selectbody = select.getSelectBody();
			SelectEval selectEval = new SelectEval(dataDir, tables,swapDir,indexDir);
			//	System.out.println("In here1");
			selectbody.accept(selectEval);
			if(selectEval.func){
				//			System.out.println("aggregate function is present");
				selectEval.oper.setFuncFlag();
			}
			if(selectEval.groupby){
				//			System.out.println("groupby is present");

			}
			//		System.out.println("In here 2");
			StatementEval stmtEval = new StatementEval();

			stmtEval.dump(selectEval.oper);
		}
	}

	@Override
	public void visit(Delete arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Update arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Insert arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(Replace arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(Drop arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(Truncate arg0) {
		// TODO Auto-generated method stub
	}
	@Override
	public void visit(CreateTable createTable) {
		ArrayList<Index> index = null;
		ScanOperator oper = null;

		indices.add(index);
		tables.put(createTable.getTable().getName().toUpperCase(), createTable);
		String originalName = createTable.getTable().getName().toUpperCase();
		List <ColumnDefinition> temp = createTable.getColumnDefinitions();
		if(buildFlag)
		{
			if(createTable.getTable().getName().toUpperCase().contains("LINEITEM") || createTable.getTable().getName().toUpperCase().contains("ORDERS") )
			{
				File file = new File(dataDir,originalName+".dat");
				oper = new ScanOperator(file, temp,null);
				if(originalName.contains("LINEITEM"))
				{
					for(int i =0;i < temp.size();i++)
					{
						if(temp.get(i).getColumnName().toString().equalsIgnoreCase("receiptdate"))
						{
							split.splitFiles(oper,temp.get(i).getColumnName(),i,indexDir,originalName);
						}
					}
				}
				
				if(originalName.contains("ORDERS"))
				{
					for(int i =0;i < temp.size();i++)
					{
						if(temp.get(i).getColumnName().toString().equalsIgnoreCase("orderdate"))
						{
							split.splitFiles(oper,temp.get(i).getColumnName(),i,indexDir,originalName);
						}
					}
				}
			}
		}
		if(buildFlag){
			if(createTable.getIndexes()!= null){
				index = (ArrayList)createTable.getIndexes();
				CreateIndex c = new CreateIndex(createTable.getTable().getName().toUpperCase(),temp,index,indexDir,dataDir);
				c.createIndices();
				//		c.readIndex();
			}		
		}
	}
}