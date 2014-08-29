package edu.buffalo.cse562;

import java.util.ArrayList;
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

public class EvaluatorForSelectPushDown implements ExpressionVisitor{

	public int[] keyForEquiJoin = null;
	public String[] indexForEquiJoin = null;
	private boolean equalsOp = false;
	private Column[] schema1;
	private HashMap<String, String[]> indexInfo;

	private String argCol;
	private Table argTable;

	int colIndex = -1;
	String indexName ;
	private boolean indexFound = false;
	private boolean constantPart = false;
	ArrayList<SelectPushDownBean> conditions;
	Datum accumulator;
	//private boolean isFirst;

	public EvaluatorForSelectPushDown(Column[] schema1) {
		this.schema1 = schema1;
		this.indexForEquiJoin = new String[2];
		this.indexInfo = UtilQE.getIndexInfo();
		conditions = new ArrayList<SelectPushDownBean>();
		//this.isFirst = isFirst;
	}

	public void reset()
	{
		keyForEquiJoin = null;
	}

	@Override
	public void visit(NullValue arg0) {
		//System.out.println("Error!! Cannot Handle this NullValue query");

	}

	@Override
	public void visit(Function arg0) {
			String funName = arg0.getName();
		if (funName.equalsIgnoreCase("date"))
		{
			constantPart = true;
			//((Expression)arg0.getParameters().getExpressions().get(0)).accept(this);
			String dateStr = arg0.getParameters().getExpressions().get(0).toString();
			dateStr = dateStr.substring(1, dateStr.length()-1);
			accumulator = new Datum.DatumDate(dateStr);
		}
		 
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
		accumulator=new Datum.DatumDouble(arg0.getValue());
		constantPart = true;
	}

	@Override
	public void visit(LongValue arg0) {
		constantPart = true;
		accumulator=new Datum.DatumLong(arg0.getValue());
	}

	@Override
	public void visit(DateValue arg0) {
		constantPart = true;
		accumulator=new Datum.DatumDate(arg0.getValue());
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
		constantPart = true;
		accumulator =new Datum.DatumString(arg0.toString());
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

		//reset();
		colIndex = -1;
		//this.equalsOp = true;
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		//String[] condition = new String[3];
		if(indexFound && constantPart){
			String colName = arg0.getLeftExpression().toString();
			String condOper = "=";
			Datum constVal = accumulator;
			conditions.add(new SelectPushDownBean(condOper, colName, constVal));
		}
		indexFound = false;
		constantPart = false;
		
		//this.equalsOp = false;
	}

	@Override
	public void visit(GreaterThan arg0) {
		//	System.out.println("in greater than");
		colIndex = -1;
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		if(indexFound && constantPart){
			String colName = arg0.getLeftExpression().toString();
			String condOper = ">";
			Datum constVal = accumulator;
			conditions.add(new SelectPushDownBean(condOper, colName, constVal));
		}
		indexFound = false;
		constantPart = false;
		
	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		colIndex = -1;
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		if(indexFound && constantPart){
			String colName = arg0.getLeftExpression().toString();
			String condOper = ">=";
			Datum constVal = accumulator;
			conditions.add(new SelectPushDownBean(condOper, colName, constVal));
		}
		indexFound = false;
		constantPart = false;
		
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
		colIndex = -1;
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		if(indexFound && constantPart){
			String colName = arg0.getLeftExpression().toString();
			String condOper = "<";
			Datum constVal = accumulator;
			conditions.add(new SelectPushDownBean(condOper, colName, constVal));
		}
		indexFound = false;
		constantPart = false;
		
	}

	@Override
	public void visit(MinorThanEquals arg0) {
		colIndex = -1;
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		if(indexFound && constantPart){
			String colName = arg0.getLeftExpression().toString();
			String condOper = "<=";
			Datum constVal = accumulator;
			conditions.add(new SelectPushDownBean(condOper, colName, constVal));
		}
		indexFound = false;
		constantPart = false;
		
	}

	@Override
	public void visit(NotEqualsTo arg0) {
		colIndex = -1;
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		String[] condition = new String[3];
		if(indexFound && constantPart){
			String colName = arg0.getLeftExpression().toString();
			String condOper = "<>";
			Datum constVal = accumulator;
			conditions.add(new SelectPushDownBean(condOper, colName, constVal));
		}
		indexFound = false;
		constantPart = false;
		
	}

	@Override
	public void visit(Column arg0) {
		argCol = arg0.getWholeColumnName();
		argTable = arg0.getTable();
		String tableName = null;
		//String columnName = arg0.getColumnName();
		boolean found = false;
		//boolean tableFound = false;
		boolean tableNameIsAlias=false;
		colIndex = -1;

		for(int i = 0; i < schema1.length; i++){
			if(schema1[i] == null) break;
			if(schema1[i].getWholeColumnName().equals(argCol)){
				colIndex = i;
				found = true;
				break;
			}
		}
		//Check if table name is alias
		if(found){
			Set<String> keySet = (Set<String>)FromScanner.tableNameMapping.keySet();
			for(String key :keySet)
			{
				if(key.equals(argTable.getName()))
				{
					tableNameIsAlias = true;
					tableName = FromScanner.tableNameMapping.get(key).toUpperCase();
				}
			}

			///code to find the index file name
			//Retrieve the names from the schema based on index
			String tableNameInShema = "";
			String colNameInSchema = "";
			//for left index

			tableNameInShema = schema1[colIndex].getTable().getName().toUpperCase();
			colNameInSchema = schema1[colIndex].getColumnName();

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
							indexName = tableName + "_" + colname;
						}
						indexPresent = true;
						indexFound = true;
						break;
					}
				}			
			}			
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
		arg0.getExpression().accept(this);
	}
}