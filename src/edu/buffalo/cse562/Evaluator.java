package edu.buffalo.cse562;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import edu.buffalo.cse562.Datum.DatumDouble;
import edu.buffalo.cse562.Datum.DatumLong;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubSelect;

public class Evaluator implements ExpressionVisitor{

	private boolean isTrue=false;
	private Column[] schema;
	private ArrayList<Datum> tuple;
	private static ArrayList<Datum> tempTuple;
	private Datum accumulator;
	private File dataDir;
	private File swapDir;
	private HashMap<String, CreateTable> tables;
	private TreeMap<String, ArrayList<Datum>> groupHashMap;
	private static boolean isFunction = false;
	private int colIndex;
	private static int count;
	private boolean isGroupBy;
	private String groupKey;
	private int groupIndex;
	private String[] aliasList;
	private List selectColList;
	private int globalCount = 0;
	private boolean isJoinWhereFlag = false;
	private int schemaLength;
	ArrayList<Integer> groupBySchema;
	Map<String, Set<Datum>> groupDistinctHashMap;
	ArrayList<Integer> mappedIndex;
	Boolean mappingDone = false;
	int currentMappingIndex = 0 ;

	//private List selectColList;
	public boolean getBool(){
		return this.isTrue;
	}
	public Datum getAccumulator(){
		return accumulator;
	}
	public void setMappingFlag(){
		mappingDone = true;
	}
	public Datum setAccumulator(){
		return accumulator;
	}
	public static boolean getIsFunction(){
		return isFunction;
	}

	public Evaluator(Column[] schema, ArrayList<Datum> tuple,HashMap<String, CreateTable> tables,File dataDir, TreeMap<String, ArrayList<Datum>> groupHashMap, boolean isgroupBy, String [] aliasList, List selectColList, File swapDir,Map<String,Set<Datum>> groupDistinctHashMap,ArrayList<Integer> groupBySchema) {
		this.schema = schema;
		this.tuple = tuple;
		this.dataDir = dataDir;
		this.tables = tables;
		this.groupHashMap = groupHashMap;
		this.isGroupBy = isgroupBy;
		this.aliasList = aliasList;
		this.selectColList = selectColList;
		//this.groupBySchemaDataTypes = new ArrayList<ColumnDefinition>();
		this.groupBySchema = groupBySchema;
		this.groupDistinctHashMap = groupDistinctHashMap;
		this.mappedIndex = new ArrayList<Integer>();
	}
	
	public void setTuple(ArrayList<Datum> tuple){
		this.tuple = tuple;
	}
	
	public void setJoinWhereFlag(){
		isJoinWhereFlag = true;
	}
	
	public void setSchemaLength(int len){
		schemaLength = len;
	}
	
	public void setGroupKey(String key){
		groupKey = key;
	}
	
	public void setIndex(int index){
		groupIndex = index;
	}
	
	public static void resetAll(){
		count = 0;
		tempTuple = null;
		
	}
	
	public void incrementCount(){
		count++;
	}
	
	public void setColumnIndex(int colIndex){
		this.colIndex = colIndex;
	}

	/*public ArrayList<ColumnDefinition> getGroupBySchemaDataTypes(){
		return groupBySchemaDataTypes;
	}*/

	@Override
	public void visit(NullValue arg0) {
		
	}
	public ArrayList<Integer> getGroupBySchema(){
		return groupBySchema;
	}
	@Override
	public void visit(Function arg0) {
		//isFunction = true;
		String funName = arg0.getName();
		String groupByDataType = null;
		Integer groupByColName = null;
		//maintain the temp tuple
		if (funName.equalsIgnoreCase("date")){
			((Expression)arg0.getParameters().getExpressions().get(0)).accept(this);
		}
		else
		{
			isFunction = true;
		}
		/*if(!mappingDone){
			((Expression)arg0.getParameters().getExpressions().get(0)).accept(this);
			return;
		}*/			
		if(isGroupBy){
			if(groupHashMap.containsKey(groupKey)){
				ArrayList<Datum> projectedList = groupHashMap.get(groupKey);
				int count = ((DatumLong)projectedList.get(projectedList.size() -1)).getValue().intValue();
				projectedList.set(projectedList.size()-1, new DatumLong(count));
				if(funName.equalsIgnoreCase("count")){
					if(arg0.isDistinct()){
						((Expression)arg0.getParameters().getExpressions().get(0)).accept(this);
						groupDistinctHashMap.get(groupKey).add(accumulator);							
						groupByColName = 6;					
						projectedList.set(groupIndex, new DatumLong(groupDistinctHashMap.get(groupKey).size()));						
					}else{
						groupByColName = 2;	
						projectedList.set(groupIndex, new DatumLong(count));						
					}// sum = 1, count = 2, avg = 3, min = 4, max = 5,countDistinct = 6,key = 0,col = 99 
					
				}else{
					((Expression)arg0.getParameters().getExpressions().get(0)).accept(this);
					if(accumulator instanceof Datum.DatumLong){

						if(funName.equalsIgnoreCase("sum")){
							long tempVal = ((DatumLong)projectedList.get(groupIndex)).getValue().longValue();
							projectedList.set(groupIndex, new Datum.DatumLong(tempVal + ((DatumLong)accumulator).getValue().longValue()));
							groupByColName = 1;
						}
						else if(funName.equalsIgnoreCase("avg")){
							double sum; 
							if(projectedList.get(groupIndex) instanceof Datum.DatumLong){
								sum = ((((DatumLong)projectedList.get(groupIndex)).getValue().longValue() * (count-1)) + ((DatumLong)accumulator).getValue().longValue());
							}
							else{
								sum = ((((DatumDouble)projectedList.get(groupIndex)).getValue().doubleValue() * (count-1)) + ((DatumLong)accumulator).getValue().longValue());
							}
							groupByColName = 3;
							projectedList.set(groupIndex, new Datum.DatumDouble(sum*1.0/count));
						}
						else if(funName.equalsIgnoreCase("min")){
							long min = ((DatumLong)projectedList.get(groupIndex)).getValue().longValue();
							if(((DatumLong)accumulator).getValue().longValue() < min){
								projectedList.set(groupIndex, new Datum.DatumLong(((DatumLong)accumulator).getValue().longValue()));
							}
							groupByColName = 4;
						}
						else if(funName.equalsIgnoreCase("max")){
							long max = ((DatumLong)projectedList.get(groupIndex)).getValue().longValue();
							if(((DatumLong)accumulator).getValue().longValue() > max){
								projectedList.set(groupIndex, new Datum.DatumLong(((DatumLong)accumulator).getValue().longValue()));
							}
							groupByColName = 5;
						}						
						accumulator = projectedList.get(groupIndex);
					}
					else if(accumulator instanceof Datum.DatumDouble){
						
						if(funName.equalsIgnoreCase("sum")){
							double tempVal = ((DatumDouble)projectedList.get(groupIndex)).getValue().doubleValue();
							projectedList.set(groupIndex, new Datum.DatumDouble(tempVal + ((DatumDouble)accumulator).getValue().doubleValue()));
							groupByColName = 1;
						}
						else if(funName.equalsIgnoreCase("avg")){
							double sum = ((((DatumDouble)projectedList.get(groupIndex)).getValue().doubleValue() * (count-1)) + ((DatumDouble)accumulator).getValue().doubleValue());
							projectedList.set(groupIndex, new Datum.DatumDouble(sum*1.0/count));
							groupByColName = 3;
						}
						else if(funName.equalsIgnoreCase("min")){
							double min = ((DatumDouble)projectedList.get(groupIndex)).getValue().doubleValue();
							if(((DatumDouble)accumulator).getValue().doubleValue() < min){
								projectedList.set(groupIndex, new Datum.DatumDouble(((DatumDouble)accumulator).getValue().doubleValue()));
							}
							groupByColName = 4;
						}
						else if(funName.equalsIgnoreCase("max")){
							double max = ((DatumDouble)projectedList.get(groupIndex)).getValue().doubleValue();
							if(((DatumDouble)accumulator).getValue().doubleValue() > max){
								projectedList.set(groupIndex, new Datum.DatumDouble(((DatumDouble)accumulator).getValue().doubleValue()));
							}
							groupByColName = 5;
						}
					}
				}
				if((groupIndex+1) > groupBySchema.size()){
					createGroupBySchema(groupByColName,groupIndex,projectedList.size());
				}
			}else{
				if(funName.equalsIgnoreCase("count")){
					if(arg0.isDistinct()){
						((Expression)arg0.getParameters().getExpressions().get(0)).accept(this);
						Set<Datum> distinctValues = new HashSet<Datum>();
						distinctValues.add(accumulator);							
						groupDistinctHashMap.put(groupKey,distinctValues);
						groupByColName = 6;
					}else{// sum = 1, count = 2, avg = 3, min = 4, max = 5,countDistinct = 6,key = 0,col = 99 
						
						groupByColName = 2;						
					}
					accumulator = new Datum.DatumLong(1);
				}
				else{
					((Expression)arg0.getParameters().getExpressions().get(0)).accept(this);
					if(funName.equalsIgnoreCase("sum"))					
						groupByColName = 1;
					else if(funName.equalsIgnoreCase("avg"))
						groupByColName = 3;
					else if(funName.equalsIgnoreCase("min"))
						groupByColName = 4;
					else if(funName.equalsIgnoreCase("max"))
						groupByColName = 5;				
				}
				if((groupIndex+1) > groupBySchema.size()){
					createGroupBySchema(groupByColName,groupIndex,selectColList.size()+1);
				}				
			}			
		}
		else{
			if(tempTuple == null){
				//create new temp tuple
				tempTuple = new ArrayList<Datum>();
				for(Object data : selectColList){
					tempTuple.add(null);
				}
				tempTuple.add(new Datum.DatumLong(0));
			}
			if(arg0.isAllColumns()){
				accumulator = new Datum.DatumLong(count);
			}
			else{
				((Expression)arg0.getParameters().getExpressions().get(0)).accept(this);
				if(funName.equalsIgnoreCase("count")){
					accumulator = new Datum.DatumLong(count);
				}
				else if(accumulator instanceof Datum.DatumLong){
					if(tempTuple.get(colIndex) == null){
						tempTuple.add(colIndex, new Datum.DatumLong(0));
					}
					if(funName.equalsIgnoreCase("sum")){
						long tempVal = ((DatumLong)tempTuple.get(colIndex)).getValue().longValue();
						tempTuple.set(colIndex, new Datum.DatumLong(tempVal +((DatumLong)accumulator).getValue().longValue()));
					}
					else if(funName.equalsIgnoreCase("avg")){
						double sum; 

						if(tempTuple.get(colIndex) instanceof Datum.DatumLong){
							sum = ((((Datum.DatumLong)tempTuple.get(colIndex)).getValue().doubleValue() * (count-1)) + ((Datum.DatumLong)accumulator).getValue().longValue());
						}
						else{
							sum = ((((Datum.DatumDouble)tempTuple.get(colIndex)).getValue().doubleValue() * (count-1)) + ((Datum.DatumLong)accumulator).getValue().longValue());
						}

						tempTuple.set(colIndex, new Datum.DatumDouble(sum*1.0/count));
					}
					else if(funName.equalsIgnoreCase("min")){
						long min = ((DatumLong)tempTuple.get(colIndex)).getValue().longValue();
						if(((DatumLong)accumulator).getValue().longValue() < min){
							tempTuple.set(colIndex, new Datum.DatumLong(((DatumLong)accumulator).getValue().longValue()));
						}
					}
					else if(funName.equalsIgnoreCase("max")){
						long max = ((DatumLong)tempTuple.get(colIndex)).getValue().longValue();
						if(((DatumLong)accumulator).getValue().longValue() > max){
							tempTuple.set(colIndex, new Datum.DatumLong(((DatumLong)accumulator).getValue().longValue()));
						}
					}

					accumulator = tempTuple.get(colIndex);
				}
				else if(accumulator instanceof Datum.DatumDouble ){
					Datum placeHolder = null;
					if(tempTuple.get(colIndex) == null){
						tempTuple.add(colIndex, new Datum.DatumLong(0));
					}
					if(funName.equalsIgnoreCase("sum")){
						if(tempTuple.get(colIndex) instanceof DatumDouble)
						{
							placeHolder = tempTuple.get(colIndex);
						}
						else
						{
							placeHolder = new DatumDouble((tempTuple.get(colIndex).getValue()).toString());
						}
						double tempVal = (double) placeHolder.getValue();
						tempTuple.set(colIndex, new Datum.DatumDouble(tempVal + ((DatumDouble)accumulator).getValue().doubleValue()));
					}
					else if(funName.equalsIgnoreCase("avg")){
						double sum = ((((DatumDouble)tempTuple.get(colIndex)).getValue().doubleValue() * (count-1)) + ((DatumDouble)accumulator).getValue().doubleValue());
						tempTuple.set(colIndex, new Datum.DatumDouble(sum*1.0/count));
					}
					else if(funName.equalsIgnoreCase("min")){
						double min = ((DatumDouble)tempTuple.get(colIndex)).getValue().doubleValue();
						if(((DatumDouble)accumulator).getValue().doubleValue() < min){
							tempTuple.set(colIndex, new Datum.DatumDouble(((DatumLong)accumulator).getValue().doubleValue()));
						}
					}
					else if(funName.equalsIgnoreCase("max")){
						double max = ((DatumDouble)tempTuple.get(colIndex)).getValue().doubleValue();
						if(((DatumDouble)accumulator).getValue().longValue() > max){
							tempTuple.set(colIndex, new Datum.DatumDouble(((DatumLong)accumulator).getValue().doubleValue()));
						}
					}

					accumulator = tempTuple.get(colIndex);
				}
			}
		}
		if(arg0.getName().equalsIgnoreCase("date")){
			String dateStr = arg0.getParameters().getExpressions().get(0).toString();
			dateStr = dateStr.substring(1, dateStr.length()-1);
			accumulator = new Datum.DatumDate(dateStr);
		}		
	}	
	public void createGroupBySchema(Integer colCode, int index,int groupBysize){
		if(groupBySchema.size() < (index) ){
			int i ;
			for(i=groupBySchema.size(); i< index;i++){
				//String colKey = "col";
				groupBySchema.add(i,99);
			}
		}
		if(groupBySchema.size()== index){
			groupBySchema.add(index,colCode);
		}
	}
	@Override
	public void visit(InverseExpression arg0) {
	}

	@Override
	public void visit(JdbcParameter arg0) {
	}

	@Override
	public void visit(DoubleValue arg0) {
		accumulator=new Datum.DatumDouble(arg0.getValue());
		
	}

	@Override
	public void visit(LongValue arg0) {
			
		accumulator=new Datum.DatumLong(arg0.getValue());
	}

	@Override
	public void visit(DateValue arg0) {
		accumulator=new Datum.DatumDate(arg0.getValue());
	}

	@Override
	public void visit(TimeValue arg0) {
		//System.out.println("Error!! Cannot Handle TimeValue this query");
	}

	@Override
	public void visit(TimestampValue arg0) {
		//System.out.println("Error!! Cannot Handle TimestampValue this query");
	}

	@Override
	public void visit(Parenthesis arg0) {
		arg0.getExpression().accept(this);
	}

	@Override
	public void visit(StringValue arg0) {
		String temp = arg0.toString();
		temp = temp.substring(1, temp.length()-1);
		accumulator = new Datum.DatumString(temp);
	}

	@Override
	public void visit(Addition arg0) {
		arg0.getLeftExpression().accept(this);
		Datum leftvalue=  accumulator;
		arg0.getRightExpression().accept(this);
		Datum rightvalue = accumulator;
		if(leftvalue instanceof DatumLong && rightvalue instanceof DatumLong){
			accumulator = new Datum.DatumLong(((DatumLong)leftvalue).value+((DatumLong)rightvalue).value);

		}
		else if(leftvalue instanceof DatumDouble && rightvalue instanceof DatumLong){
			accumulator = new Datum.DatumDouble(((DatumDouble)leftvalue).value+((DatumLong)rightvalue).value);
		}
		else if(leftvalue instanceof DatumLong && rightvalue instanceof DatumDouble){
			accumulator = new Datum.DatumDouble(((DatumLong)leftvalue).value+((DatumDouble)rightvalue).value);
		}
		if(leftvalue instanceof DatumDouble && rightvalue instanceof DatumDouble){
			accumulator = new Datum.DatumDouble(((DatumDouble)leftvalue).value+((DatumDouble)rightvalue).value);

		}
	}

	@Override
	public void visit(Division arg0) {
		arg0.getLeftExpression().accept(this);
		Datum leftvalue=  accumulator;
		arg0.getRightExpression().accept(this);
		Datum rightvalue = accumulator;
		if(leftvalue instanceof DatumLong && rightvalue instanceof DatumLong){
			accumulator = new Datum.DatumLong(((DatumLong)leftvalue).value/((DatumLong)rightvalue).value);

		}
		else if(leftvalue instanceof DatumDouble && rightvalue instanceof DatumLong){
			accumulator = new Datum.DatumDouble(((DatumDouble)leftvalue).value/((DatumLong)rightvalue).value);
		}
		else if(leftvalue instanceof DatumLong && rightvalue instanceof DatumDouble){
			accumulator = new Datum.DatumDouble(((DatumLong)leftvalue).value/((DatumDouble)rightvalue).value);
		}
		if(leftvalue instanceof DatumDouble && rightvalue instanceof DatumDouble){
			accumulator = new Datum.DatumDouble(((DatumDouble)leftvalue).value/((DatumDouble)rightvalue).value);

		}
	}

	@Override
	public void visit(Multiplication arg0) {
		arg0.getLeftExpression().accept(this);
		Datum leftvalue=  accumulator;
		arg0.getRightExpression().accept(this);
		Datum rightvalue = accumulator;
		if(leftvalue instanceof DatumLong && rightvalue instanceof DatumLong){
			accumulator = new Datum.DatumLong(((DatumLong)leftvalue).value*((DatumLong)rightvalue).value);

		}
		else if(leftvalue instanceof DatumDouble && rightvalue instanceof DatumLong){
			accumulator = new Datum.DatumDouble(((DatumDouble)leftvalue).value*((DatumLong)rightvalue).value);
		}
		else if(leftvalue instanceof DatumLong && rightvalue instanceof DatumDouble){
			accumulator = new Datum.DatumDouble(((DatumLong)leftvalue).value*((DatumDouble)rightvalue).value);
		}
		if(leftvalue instanceof DatumDouble && rightvalue instanceof DatumDouble){
			accumulator = new Datum.DatumDouble(((DatumDouble)leftvalue).value*((DatumDouble)rightvalue).value);

		}
	}

	@Override
	public void visit(Subtraction arg0) {
		arg0.getLeftExpression().accept(this);
		Datum leftvalue=  accumulator;
		arg0.getRightExpression().accept(this);
		Datum rightvalue = accumulator;

		if(leftvalue instanceof DatumLong && rightvalue instanceof DatumLong){
			accumulator = new Datum.DatumLong(((DatumLong)leftvalue).value-((DatumLong)rightvalue).value);

		}
		else if(leftvalue instanceof DatumDouble && rightvalue instanceof DatumLong){
			accumulator = new Datum.DatumDouble(((DatumDouble)leftvalue).value-((DatumLong)rightvalue).value);
		}
		else if(leftvalue instanceof DatumLong && rightvalue instanceof DatumDouble){
			accumulator = new Datum.DatumDouble(((DatumLong)leftvalue).value-((DatumDouble)rightvalue).value);
		}
		if(leftvalue instanceof DatumDouble && rightvalue instanceof DatumDouble){
			accumulator = new Datum.DatumDouble(((DatumDouble)leftvalue).value-((DatumDouble)rightvalue).value);
		}
	}

	@Override
	public void visit(AndExpression arg0) {
		arg0.getLeftExpression().accept(this);
		boolean leftbool = this.isTrue;
		this.isTrue=false;
		arg0.getRightExpression().accept(this);
		boolean rightbool = this.isTrue;
		isTrue=leftbool&&rightbool;

	}

	@Override
	public void visit(OrExpression arg0) {
		arg0.getLeftExpression().accept(this);
		boolean leftbool = this.isTrue;
		this.isTrue=false;
		arg0.getRightExpression().accept(this);
		boolean rightbool = this.isTrue;
		this.isTrue=false;
		isTrue=leftbool||rightbool;

	}

	@Override
	public void visit(Between arg0) {
		//System.out.println("Error!! Cannot Handle this query");
		
	}

	@Override
	public void visit(EqualsTo arg0) {
		Datum rightvalue,leftvalue;
		arg0.getLeftExpression().accept(this);
		leftvalue=accumulator;
		arg0.getRightExpression().accept(this);
		rightvalue = accumulator;
		
		if(isJoinWhereFlag){
			if(leftvalue == null || rightvalue == null){
				this.isTrue = true;
				return;
			}
		}
		
		if(leftvalue.equals(rightvalue)){
			this.isTrue=true;
		}
	}

	@Override
	public void visit(GreaterThan arg0) {
		Datum leftvalue,rightvalue;
		arg0.getLeftExpression().accept(this);
		leftvalue = accumulator;
		arg0.getRightExpression().accept(this);
		rightvalue = accumulator;
		if(isJoinWhereFlag){
			if(leftvalue == null || rightvalue == null){
				this.isTrue = true;
				return;
			}
		}
		
		if(!(leftvalue.minorThan(rightvalue)||leftvalue.equals(rightvalue))){
			this.isTrue=true;
		}
		
	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		arg0.getLeftExpression().accept(this);
		Datum leftvalue = accumulator;
		arg0.getRightExpression().accept(this);
		Datum rightvalue = accumulator;
		
		if(isJoinWhereFlag){
			if(leftvalue == null || rightvalue == null){
				this.isTrue = true;
				return;
			}
		}
		
		if(!leftvalue.minorThan(rightvalue)){
			this.isTrue=true;
		}
		
	}

	@Override
	public void visit(InExpression arg0) {
		//System.out.println("Error!! Cannot Handle this query");
		
	}

	@Override
	public void visit(IsNullExpression arg0) {
		//System.out.println("Error!! Cannot Handle this query");

	}

	@Override
	public void visit(LikeExpression arg0) {
		//System.out.println("Error!! Cannot Handle this query");
		
	}

	@Override
	public void visit(MinorThan arg0) {
		arg0.getLeftExpression().accept(this);
		Datum leftvalue = accumulator;
		arg0.getRightExpression().accept(this);
		Datum rightvalue = accumulator;
		Datum placeHolder;
		
		if(isJoinWhereFlag){
			if(leftvalue == null || rightvalue == null){
				this.isTrue = true;
				return;
			}
		}
		if(leftvalue.minorThan(rightvalue)){
			this.isTrue=true;
		}
	}

	@Override
	public void visit(MinorThanEquals arg0) {
		arg0.getLeftExpression().accept(this);
		Datum leftvalue = accumulator;
		arg0.getRightExpression().accept(this);
		Datum rightvalue = accumulator;
		
		if(isJoinWhereFlag){
			if(leftvalue == null || rightvalue == null){
				this.isTrue = true;
				return;
			}
		}
		
		if(leftvalue.minorThan(rightvalue)||leftvalue.equals(rightvalue)){
			this.isTrue=true;
		}
		
	}

	@Override
	public void visit(NotEqualsTo arg0) {
		arg0.getLeftExpression().accept(this);
		Datum leftvalue=accumulator;
		arg0.getRightExpression().accept(this);
		Datum rightvalue = accumulator;
		//if(leftvalue != null && leftvalue.toString().equals("Brand#11"))
		//	System.out.println("hey");
		
		if(isJoinWhereFlag){
			if(leftvalue == null || rightvalue == null){
				this.isTrue = true;
				return;
			}
		}
		if(!leftvalue.equals(rightvalue)){
			this.isTrue=true;
		}
		
	}

	@Override
	public void visit(Column arg0) {
		
		int i=0;
		int index = 0;
		boolean foundMaching = false;
		if(!mappingDone){
			for(i = 0; i < schemaLength; i++ ){
				if(schema[i] == null)
					break;
				if(schema[i].getWholeColumnName().equalsIgnoreCase(arg0.getWholeColumnName())
						|| schema[i].getColumnName().equalsIgnoreCase(arg0.getWholeColumnName())
						|| aliasList[i].equalsIgnoreCase(arg0.getWholeColumnName())){
					foundMaching = true;
					index++;
					break;
				}
			}
			if(foundMaching){
				accumulator=tuple.get(i);
				colIndex = index - 1;
				mappedIndex.add(i);
			}
			else{
				accumulator = null;
				mappedIndex.add(-1);
			}			
		}else{
			if(currentMappingIndex < mappedIndex.size()){
				if( mappedIndex.get(currentMappingIndex) != -1){
					accumulator=tuple.get(mappedIndex.get(currentMappingIndex));
					//colIndex++;
					//= mappedIndex.get(currentMappingIndex);		
				}else{
					accumulator = null;
				}
				currentMappingIndex++;
			}else{
				accumulator = null;
			}
		}
	}

	@Override
	public void visit(SubSelect subSelect) {
		SelectBody subSelectbody = subSelect.getSelectBody();
		SelectEval selectEval = new SelectEval(dataDir, tables,swapDir,null);
		subSelectbody.accept(selectEval);
		Operator oper = selectEval.oper;
		Operator outputScan = oper.readAllTuples(false);
		accumulator = outputScan.readOneTuple().get(0);
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
	}
	
	public void visit(SelectExpressionItem arg0){
		arg0.getExpression().accept(this);
	}
	
	public void resetGroupHashMap(TreeMap<String, ArrayList<Datum>> groupHashMap){
		this.groupHashMap = null;
		this.groupHashMap = groupHashMap;
	}
	
	public void resetDistinctGroupHashMap(TreeMap<String,Set<Datum>> groupDistinctHashMap){
		this.groupDistinctHashMap = null;
		this.groupDistinctHashMap = groupDistinctHashMap;
	}
	
	public void resetEval(ArrayList<Datum> tuple) { 
		this.tuple = tuple; 
		this.isTrue=false; 
		this.isJoinWhereFlag = false; 
		this.globalCount = 0;
		this.currentMappingIndex = 0;
		if(!mappingDone){
			mappedIndex = new ArrayList<Integer>();
		}
	}
	
	

}