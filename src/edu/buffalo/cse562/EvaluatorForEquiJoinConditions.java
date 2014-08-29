package edu.buffalo.cse562;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubSelect;

public class EvaluatorForEquiJoinConditions implements ExpressionVisitor{

	public int[] keyForEquiJoin = null;
	public String[] indexForEquiJoin = null;
	private boolean equalsOp = false;
	private Column[] schema1;
	private Column[] schema2;
	private boolean joinFlag = false; 
	private HashMap<String, String[]> indexInfo;
	
	private String argCol;
	private Table argTable;
	
	private int leftColIndex = -1;
	private int rightColIndex = -1;
	private String leftIndexName;
	private String rightIndexName;
	private boolean joinDone = false;
	//private boolean isFirst;
	
	public EvaluatorForEquiJoinConditions(Column[] schema1, Column[] schema2, boolean joinFlag) {
		this.schema1 = schema1;
		this.schema2 = schema2;
		this.joinFlag = joinFlag;
		this.keyForEquiJoin = new int[]{-1,-1};
		this.indexForEquiJoin = new String[2];
		this.indexInfo = UtilQE.getIndexInfo();
		//this.isFirst = isFirst;
	}

	public void reset()
	{
		keyForEquiJoin = null;
		joinFlag = false;
	}
	
	@Override
	public void visit(NullValue arg0) {
		//System.out.println("Error!! Cannot Handle this NullValue query");

	}

	@Override
	public void visit(Function arg0) {
	/*	String funName = arg0.getName();
		if (funName.equalsIgnoreCase("date"))
		{
			((Expression)arg0.getParameters().getExpressions().get(0)).accept(this);
		}
		else
			((Expression)arg0.getParameters().getExpressions().get(0)).accept(this);
		*/
	}

	@Override
	public void visit(InverseExpression arg0) {
		//System.out.println("Error!! Cannot Handle this InverseExpression query");

	}

	@Override
	public void visit(JdbcParameter arg0) {
		//System.out.println("Error!! Cannot Handle this JdbcParameter query");

	}

	@Override
	public void visit(DoubleValue arg0) {
		//accumulator=new Datum.DatumDouble(arg0.getValue());

	}

	@Override
	public void visit(LongValue arg0) {

		//accumulator=new Datum.DatumLong(arg0.getValue());
	}

	@Override
	public void visit(DateValue arg0) {
	}

	@Override
	public void visit(TimeValue arg0) {
		System.out.println("Error!! Cannot Handle TimeValue this query");

	}

	@Override
	public void visit(TimestampValue arg0) {
		System.out.println("Error!! Cannot Handle TimestampValue this query");

	}

	@Override
	public void visit(Parenthesis arg0) {
		// TODO Auto-generated method stub
		//System.out.println(arg0+"par");
		arg0.getExpression().accept(this);

	}

	@Override
	public void visit(StringValue arg0) {

	//	accumulator =new Datum.DatumString(arg0.toString());
	}

	@Override
	public void visit(Addition arg0) {
		// TODO Auto-generated method stub
		//arg0.getLeftExpression().accept(this);
		//arg0.getRightExpression().accept(this);
	}

	@Override
	public void visit(Division arg0) {
		// TODO Auto-generated method stub
		//arg0.getLeftExpression().accept(this);
		//arg0.getRightExpression().accept(this);
	}

	@Override
	public void visit(Multiplication arg0) {
		// TODO Auto-generated method stub
		//arg0.getLeftExpression().accept(this);
		//arg0.getRightExpression().accept(this);
	}

	@Override
	public void visit(Subtraction arg0) {
		// TODO Auto-generated method stub
		//arg0.getLeftExpression().accept(this);
		//arg0.getRightExpression().accept(this);
	}

	@Override
	public void visit(AndExpression arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		
	}

	@Override
	public void visit(OrExpression arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
	}

	@Override
	public void visit(Between arg0) {
		System.out.println("Error!! Cannot Handle this query");

	}

	@Override
	public void visit(EqualsTo arg0) {
		if(joinDone){
			return;
		}
		//reset();
		leftColIndex = -1;
		rightColIndex = -1;
		this.equalsOp = true;
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		if(leftColIndex != -1 && rightColIndex != -1){
			keyForEquiJoin[0] = leftColIndex;
			keyForEquiJoin[1] = rightColIndex;
			joinDone = true;
		}
		if(leftIndexName!=null && rightIndexName!=null)
		{
			indexForEquiJoin[0] = leftIndexName;
			indexForEquiJoin[1] = rightIndexName;
			
		}
		this.equalsOp = false;
	}

	@Override
	public void visit(GreaterThan arg0) {
		//	System.out.println("in greater than");
		//arg0.getLeftExpression().accept(this);
		//arg0.getRightExpression().accept(this);
		
	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		//arg0.getLeftExpression().accept(this);
		//arg0.getRightExpression().accept(this);
	}

	@Override
	public void visit(InExpression arg0) {
		System.out.println("Error!! Cannot Handle this query");

	}

	@Override
	public void visit(IsNullExpression arg0) {
		System.out.println("Error!! Cannot Handle this query");

	}

	@Override
	public void visit(LikeExpression arg0) {
		System.out.println("Error!! Cannot Handle this query");

	}

	@Override
	public void visit(MinorThan arg0) {
		//System.out.println("in minor than");
		//arg0.getLeftExpression().accept(this);
		//arg0.getRightExpression().accept(this);
	}

	@Override
	public void visit(MinorThanEquals arg0) {
		//arg0.getLeftExpression().accept(this);
		//arg0.getRightExpression().accept(this);
	
	}

	@Override
	public void visit(NotEqualsTo arg0) {
		//arg0.getLeftExpression().accept(this);
		//arg0.getRightExpression().accept(this);
	}

	@Override
	public void visit(Column arg0) {
		argCol = arg0.getWholeColumnName();
		argTable = arg0.getTable();
		String indexes[],tableName=null;
		String columnName = arg0.getColumnName();
		boolean found = false;
		boolean tableFound = false,tableNameIsAlias=false;
		//Check if table name is alias
		Set<String> keySet = (Set<String>)FromScanner.tableNameMapping.keySet();
		for(String key :keySet)
		{
			if(key.equals(argTable.getName()))
			{
				tableNameIsAlias = true;
				tableName = FromScanner.tableNameMapping.get(key).toUpperCase();
			}
		}
		if(joinFlag && equalsOp){
			for(int i = 0; i < schema1.length; i++){
				if(schema1[i] == null) break;
				if(schema1[i].getWholeColumnName().equals(argCol)){
					leftColIndex = i;
					found = true;
					break;
				}
			}
			
			if(found!=true){
				for(int i = 0; i < schema2.length; i++){
					if(schema2[i] == null) break;
					if(schema2[i].getWholeColumnName().equals(argCol)){
						rightColIndex = i;
						break;
					}
				}
			}
			
			/*if(leftColIndex == -1){
				return;
			}
			else if(found != true && rightColIndex == -1){
				return;
			}*/
			/*///////////////////////////////////////////////////////////////////////////////////////////
			///code to find the index file name
			//Retrieve the names from the schema based on index
			String tableNameInShema = "";
			String colNameInSchema = "";
			//for left index
			if(found == true){
				if(leftColIndex != -1){
					tableNameInShema = schema1[leftColIndex].getTable().getName().toUpperCase();
					colNameInSchema = schema1[leftColIndex].getColumnName();
				}
			}
			//for right index
			else{
				if(rightColIndex != -1){
					tableNameInShema = schema2[rightColIndex].getTable().getName().toUpperCase();
					colNameInSchema = schema2[rightColIndex].getColumnName();
				}
			}
			
			String[] indexCols;
			if(tableNameIsAlias){
				indexCols = indexInfo.get(tableName);
			}
			else{
				indexCols = indexInfo.get(tableNameInShema);
				tableName = tableNameInShema;
			}
			String indexFileName = null;
			boolean indexPresent = false;
			if(indexCols != null){
				//index present for this table
				for(String colname : indexCols){
					if(colname.equals(colNameInSchema)){
						//col name contains in index
						if(found == true){
							leftIndexName = tableName + "_" + colname;
						}
						else{
							rightIndexName = tableName + "_" + colname;
						}
						
						indexPresent = true;
						break;
					}
				}
				if(!indexPresent){
					if(found == true){
						leftIndexName = tableName;
					}
					else{
						rightIndexName = tableName;
					}
					//System.out.println("Index not present for tab_col: " + tableName + "_" + colNameInSchema);
				}
			}
			else{
				//no index present for this table.
				if(found == true){
					leftIndexName = tableName;
				}
				else{
					rightIndexName = tableName;
				}
				//System.out.println("Index not present for table: " + tableName);
			}
		/////////////////////////////////////////////////////////////////////////////////////////
*/			/*found = false;
			Iterator<Map.Entry<String, String[]>> iter = indexInfo.entrySet().iterator();
			while(iter.hasNext() && found == false && tableNameIsAlias == false)
			{
				Map.Entry<String, String[]> entry = iter.next();
				String key = entry.getKey();
				if(argTable.equals(key))
				{
					indexes = entry.getValue();
					for(int j=0;j<indexes.length;j++)
					{
						if(indexes[j].equals(columnName))
						{
							leftIndexName = argTable.getName()+"_"+columnName;
							found = true;
							break;
						}
					}
					
				}
				
			}
			iter = indexInfo.entrySet().iterator();
			while(iter.hasNext() && found == false && tableNameIsAlias == true)
			{
				Map.Entry<String, String[]> entry = iter.next();
				String key = entry.getKey();
				if(tableName.equals(key))
				{
					indexes = entry.getValue();
					for(int j=0;j<indexes.length;j++)
					{
						if(indexes[j].equals(columnName))
						{
							leftIndexName = tableName+"_"+columnName;
							found = true;
							break;
						}
					}
					
				}
			}
			if(found!=true)
			{
				iter = indexInfo.entrySet().iterator();
				while(iter.hasNext() && found == false && tableNameIsAlias == false)
				{
					Map.Entry<String, String[]> entry = iter.next();
					String key = entry.getKey();
					if(argTable.equals(key))
					{
						indexes = entry.getValue();
						for(int j=0;j<indexes.length;j++)
						{
							if(indexes[j].equals(columnName))
							{
								rightIndexName = argTable.getName()+"_"+columnName;
								found = true;
								break;
							}
						}
						
					}
					
				}
				iter = indexInfo.entrySet().iterator();
				while(iter.hasNext() && found == false && tableNameIsAlias == true)
				{
					Map.Entry<String, String[]> entry = iter.next();
					String key = entry.getKey();
					if(tableName.equals(key))
					{
						indexes = entry.getValue();
						for(int j=0;j<indexes.length;j++)
						{
							if(indexes[j].equals(columnName))
							{
								rightIndexName = tableName+"_"+columnName;
								found = true;
								break;
							}
						}
						
					}
				}
			}*/
		}
	}
	

	@Override
	public void visit(SubSelect subSelect) {
		// TODO Auto-generated method stub
		//subSelectbody.accept(selectEval);		
	}

	@Override
	public void visit(CaseExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(WhenClause arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(ExistsExpression arg0) {
		// TODO Auto-generated method stub


	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Concat arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Matches arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseAnd arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseOr arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseXor arg0) {
		// TODO Auto-generated method stub

	}

	public void visit(ExpressionList arg0){
		//		System.out.println(" in list");
	}

	public void visit(SelectExpressionItem arg0){
		//		System.out.println("in select exp item");
		arg0.getExpression().accept(this);
	}
}