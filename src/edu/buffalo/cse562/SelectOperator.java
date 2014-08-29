package edu.buffalo.cse562;

import java.io.*;
import java.io.ObjectInputStream.GetField;
import java.util.*;
import java.util.Map.Entry;

import edu.buffalo.cse562.Datum.DatumDate;
import edu.buffalo.cse562.Datum.DatumDouble;
import edu.buffalo.cse562.Datum.DatumLong;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.statement.create.table.*;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.expression.*;
import edu.buffalo.cse562.Datum.DatumString;

public class SelectOperator implements Operator {

	boolean isFunc=false;
	ArrayList<Operator> input;
	ArrayList <Column[]> schema;
	Column [] tempSchema;
	private Expression condition;
	private List selectColList;
	private List groupbylist;
	private List<OrderByElement> orderbylist;
	private File dataDir;
	private File swapDir;
	//private static int joinCount=0;
	private HashMap<String, CreateTable> tables;
	private String[] tupleAliasList;
	ArrayList<ArrayList<Datum>> projectedTuples = null;
	boolean isColsDefCreated = false;
	boolean isSelectAll = false;
	private int[] keysForJoin = null;
	private String[] keysForIndex = null;
	int groupByFileCount = 0;
	private long limitCount; 
	long startTime = 0;
	boolean doNestedJoin = false;
	Operator operatorForScan = null;
	boolean printToConsole = false;
	//Data Structures added for indexing
	//private static File propertyFile =new File("/config.properties");
	//private static Properties properties ;
	//private HashMap<String,String[]> indexInfo = new HashMap<String,String[]>();
	boolean isMapCreated = false;

	public SelectOperator ( ArrayList<Operator> input, ArrayList<Column[]> schema, 
			List<SelectExpressionItem> selectColList, Expression condition, 
			HashMap<String, CreateTable> tables,File dataDir, List groupby, 
			List<OrderByElement> orderby, File swapDir,Long limitCount)
	{
		this.input = input;
		this.schema = schema;
		this.condition = condition;
		this.selectColList = selectColList;
		this.dataDir = dataDir;
		this.swapDir = swapDir;
		this.tables = tables;
		this.groupbylist=groupby;
		this.orderbylist=orderby;
		this.limitCount = limitCount;
		Evaluator.resetAll();
		projectedTuples = new ArrayList<ArrayList<Datum>>();
	}

	public int getsize(){
		return 0;
	}

	public void close()
	{
		//do nothing
	}

	@Override
	public ArrayList<Datum> readOneTuple() {
		return null;
	}

	public boolean joinExists(){

		if (schema.size()>1)
			return true;
		else 
			return false;
	}

	/**
	 * Put all the tuples of the given operator in the hashmap
	 * @param operPosition the operator position to find key from keysForJoin
	 *  */
	private HashMap<Datum, ArrayList<ArrayList<Datum>>> createHashMapForJoin(Operator operator, Column[] schemaForOper, int operPosition){

		Evaluator evalWhere = new Evaluator ( schemaForOper, null,tables,dataDir,null, false, tupleAliasList, selectColList,swapDir,null,null);
		boolean setMappingFlag = false;
		HashMap<Datum, ArrayList<ArrayList<Datum>>> hashMapForBucket = new HashMap<Datum, ArrayList<ArrayList<Datum>>>();
		ArrayList<Datum> tuple = null;
		while((tuple = operator.readOneTuple()) != null){
			evalWhere.resetEval(tuple);
			evalWhere.setJoinWhereFlag();
			evalWhere.setSchemaLength(tuple.size());
			if(condition !=null){
				condition.accept(evalWhere);
				if(!setMappingFlag){
					evalWhere.setMappingFlag();
					setMappingFlag = true;
				}
				if(!evalWhere.getBool()){
					continue;
				}
			}
			Datum key = tuple.get(keysForJoin[operPosition]);
			if(hashMapForBucket.containsKey(key)){
				ArrayList<ArrayList<Datum>> listForKey = hashMapForBucket.get(key);
				listForKey.add(tuple);
			}
			else{
				ArrayList<ArrayList<Datum>> listForKey = new ArrayList<ArrayList<Datum>>();
				listForKey.add(tuple);
				hashMapForBucket.put(key, listForKey);
			}
		}
		return hashMapForBucket;
	}

	private Operator joinWithIndex(Operator operForIter, Operator operForIndex, Column[] schemaForIter, Column[] schemaForIndex,
			boolean isSwapSchemas, int keyPosition, List<ColumnDefinition> colsDef){
		ArrayList<Datum> tupleFromOper = null;
		boolean iscoldefsCreated = false;
		boolean setMappingFlag = false;
		Evaluator evalWhere = new Evaluator ( schemaForIter, null,tables,dataDir,null, false, tupleAliasList, selectColList,swapDir,null,null);
		Evaluator evalWhereIndex = new Evaluator ( schemaForIndex, null,tables,dataDir,null, false, tupleAliasList, selectColList,swapDir,null,null);

		ArrayList<ArrayList<Datum>> tempTupleList = new ArrayList<ArrayList<Datum>>();
		while((tupleFromOper = operForIter.readOneTuple()) != null){
			Datum key = tupleFromOper.get(keysForJoin[keyPosition]);
			//check if the selection is valid for the tupleFromOper tuple
			evalWhere.resetEval(tupleFromOper);
			evalWhere.setJoinWhereFlag();
			evalWhere.setSchemaLength(tupleFromOper.size());

			if(condition !=null){
				condition.accept(evalWhere);
				if(!setMappingFlag){
					evalWhere.setMappingFlag();
					setMappingFlag = true;
				}
				if(!evalWhere.getBool()){
					continue;
				}
			}

			ArrayList<ArrayList<Datum>> tuplesFromIndex = ((IndexOperator)operForIndex).getValueForKey(key);
			if(tuplesFromIndex != null){
				for(ArrayList<Datum> row : tuplesFromIndex){
					if(row != null){
						evalWhereIndex.resetEval(row);
						evalWhereIndex.setJoinWhereFlag();
						evalWhereIndex.setSchemaLength(row.size());
						if(condition !=null){
							condition.accept(evalWhereIndex);
							if(!setMappingFlag){
								evalWhereIndex.setMappingFlag();
								setMappingFlag = true;
							}
							if(!evalWhereIndex.getBool()){
								continue;
							}
						}

						ArrayList<Datum> joinedTuple = new ArrayList<Datum>(tupleFromOper.size() + row.size());
						if(!isSwapSchemas){
							joinedTuple.addAll(tupleFromOper);
							joinedTuple.addAll(row);
						}
						else{
							joinedTuple.addAll(row);
							joinedTuple.addAll(tupleFromOper);
						}

						// create colsdef list for scan operator if not already created
						if(!iscoldefsCreated && colsDef == null){
							iscoldefsCreated = true;
							colsDef = createColDef(joinedTuple);
						}

						tempTupleList.add(joinedTuple);
					}
				}
			}

		}

		Operator outputscan = new InMemoryOperator(tempTupleList);
		return outputscan;
	}

	private Operator joinWithHash(HashMap<Datum, ArrayList<ArrayList<Datum>>> hashMapForJoin, Column[] schemaForScan,
			boolean isSwapSchemas, int keyPosition, List<ColumnDefinition> colsDef, Column[] schema3){
		//System.out.println("entered joinWithHash");
		ArrayList<Datum> tupleFromOper = null;
		boolean iscoldefsCreated = false;
		ArrayList<ArrayList<Datum>> tempTupleList = null;
		HashMap<Datum, ArrayList<ArrayList<Datum>>> tempHashMap = null;
		int[] keysForNextJoin = null;
		// if this is the last join, the create in memory operator
		if(schema3 == null){
			tempTupleList = new ArrayList<ArrayList<Datum>>();
			isMapCreated = false;
			//System.out.println("new map not created");
		}
		else{
			//System.out.println("creating new map");
			tempHashMap = new HashMap<Datum, ArrayList<ArrayList<Datum>>>();
			//check for the join condition on the next schema
			EvaluatorForEquiJoinConditions equiEval = new EvaluatorForEquiJoinConditions(tempSchema, schema3, true);
			if(condition !=null){
				condition.accept(equiEval);
			}
			keysForNextJoin = equiEval.keyForEquiJoin;
			//String[] keysForNextIndex = equiEval.indexForEquiJoin;
			isMapCreated = true;
		}

		Evaluator evalWhere = new Evaluator ( schemaForScan, null,tables,dataDir,null, false, tupleAliasList, selectColList,swapDir,null,null);
		boolean setMappingFlag = false;

		while((tupleFromOper = operatorForScan.readOneTuple()) != null){
			//System.out.println(tupleFromOper);
			Datum key = tupleFromOper.get(keysForJoin[keyPosition]);
			//check if the selection is valid for the tupleFromOper tuple
			/*evalWhere.resetEval(tupleFromOper);
			evalWhere.setJoinWhereFlag();
			evalWhere.setSchemaLength(tupleFromOper.size());

			if(condition !=null){
				condition.accept(evalWhere);
				if(!setMappingFlag){
					evalWhere.setMappingFlag();
					setMappingFlag = true;
				}
				if(!evalWhere.getBool()){
					continue;
				}
			}*/
			
			if(hashMapForJoin.containsKey(key)){
				evalWhere.resetEval(tupleFromOper);
				evalWhere.setJoinWhereFlag();
				evalWhere.setSchemaLength(tupleFromOper.size());

				if(condition !=null){
					condition.accept(evalWhere);
					if(!setMappingFlag){
						evalWhere.setMappingFlag();
						setMappingFlag = true;
					}
					if(!evalWhere.getBool()){
						continue;
					}
				}
				
				ArrayList<ArrayList<Datum>> listForKey = hashMapForJoin.get(key);
				for(ArrayList<Datum> tupleFromHashMap : listForKey){

					ArrayList<Datum> joinedTuple = new ArrayList<Datum>(tupleFromHashMap.size() + tupleFromOper.size());
					if(!isSwapSchemas){
						joinedTuple.addAll(tupleFromHashMap);
						joinedTuple.addAll(tupleFromOper);
					}
					else{
						joinedTuple.addAll(tupleFromOper);
						joinedTuple.addAll(tupleFromHashMap);
					}

					// create colsdef list for scan operator if not already created
					if(!iscoldefsCreated && colsDef == null){
						iscoldefsCreated = true;
						colsDef = createColDef(joinedTuple);
					}
					
					////if schema 3 present then put in hash
					if(schema3 != null){
						Datum newKey = joinedTuple.get(keysForNextJoin[0]);
						if(tempHashMap.containsKey(newKey)){
							ArrayList<ArrayList<Datum>> listForNewKey = tempHashMap.get(newKey);
							listForNewKey.add(joinedTuple);
						}
						else{
							ArrayList<ArrayList<Datum>> listForNewKey = new ArrayList<ArrayList<Datum>>();
							listForNewKey.add(joinedTuple);
							tempHashMap.put(newKey, listForNewKey);
						}
					}
					else{
						tempTupleList.add(joinedTuple);
					}
				}
			}

		}
		Operator outputscan = null;
		if(schema3 == null){
			outputscan = new InMemoryOperator(tempTupleList);
		}
		else{
			outputscan = new MapOperator(tempHashMap);
		}
		
		return outputscan;
	}

	public Operator joinTwoIndexSortMerge(Operator oper1, Operator oper2, Column[] schema1, Column[] schema2){
		return null;
	}

	public Operator indexJoin(Operator oper1, Operator oper2, Column[] schema1, Column[] schema2, boolean isFirstIterOper){
		List<ColumnDefinition> colsDef = null;
		boolean isSwapSchemas = false;
		Operator operatorForIter = null;
		Operator operatorForIndex = null;
		Column[] schemaForIter = null;
		Column[] schemaForIndex = null;
		int operPosition = 0;
		int keyPosForOperScan = 0;


		if(isFirstIterOper){
			//hash on oper1
			operatorForIter = oper1;
			schemaForIter = schema1;
			operatorForIndex = oper2;
			schemaForIndex = schema2;
			operPosition = 0;
			isSwapSchemas = false;
			keyPosForOperScan = 0;
		}
		else{
			//hash on oper2
			operatorForIter = oper2;
			schemaForIter = schema2;
			operatorForIndex = oper1;
			schemaForIndex = schema1;
			operPosition = 1;
			isSwapSchemas = true;
			keyPosForOperScan = 1;
		}


		Operator outputscan = null;
		//HashMap<Datum, ArrayList<ArrayList<Datum>>> hashMapForOper = createHashMapForJoin(operatorForHash, schemaForHash, operPosition);
		outputscan = joinWithIndex(operatorForIter, operatorForIndex, schemaForIter, schemaForIndex, isSwapSchemas, keyPosForOperScan, colsDef);
		//operatorForHash.close();
		//operatorForScan.close();
		//hashMapForOper = null;

		return outputscan;
	}

	public Operator joinTwoTablesBlockHash(Operator oper1, Operator oper2, Column[] schema1, Column[] schema2, Column[] schema3){
		List<ColumnDefinition> colsDef = null;
		//find least size
		int size1 = oper1.getsize();
		int size2 = oper2.getsize();
		boolean isSwapSchemas = false;
		Operator operatorForHash = null;
		Column[] schemaForHash = null;
		Column[] schemaForScan = null;
		int operPosition = 0;
		int keyPosForOperScan = 0;
		
		if(isMapCreated){
			operatorForHash = oper1;
			schemaForHash = schema1;
			operatorForScan = oper2;
			schemaForScan = schema2;
			operPosition = 0;
			isSwapSchemas = false;
			keyPosForOperScan = 1;
		}
		else{
			if(size1 < size2){
				//hash on oper1
				operatorForHash = oper1;
				schemaForHash = schema1;
				operatorForScan = oper2;
				schemaForScan = schema2;
				operPosition = 0;
				isSwapSchemas = false;
				keyPosForOperScan = 1;
			}
			else{
				//hash on oper2
				operatorForHash = oper2;
				schemaForHash = schema2;
				operatorForScan = oper1;
				schemaForScan = schema1;
				operPosition = 1;
				isSwapSchemas = true;
				keyPosForOperScan = 0;
			}
		}
		
		

		Operator outputscan = null;
		HashMap<Datum, ArrayList<ArrayList<Datum>>> hashMapForOper = null;
		if(isMapCreated){
			hashMapForOper = ((MapOperator)operatorForHash).getHashMap();
		}
		else{
			hashMapForOper = createHashMapForJoin(operatorForHash, schemaForHash, operPosition);
		}
		
		outputscan = joinWithHash(hashMapForOper,schemaForScan,isSwapSchemas,keyPosForOperScan, colsDef, schema3);	
		//operatorForHash.close();
		//operatorForScan.close();
		hashMapForOper = null;

		return outputscan;
	}

	private int joinTwoSchemas(Column[] schema1, Column[] schema2, int currSize){
		EvaluatorForEquiJoinConditions equiEval = new EvaluatorForEquiJoinConditions(schema1, schema2, true);
		if(condition !=null){
			condition.accept(equiEval);
		}
		keysForJoin = equiEval.keyForEquiJoin;
		keysForIndex = equiEval.indexForEquiJoin;
		if(keysForJoin[0] == -1 || keysForJoin[1] == -1){
			doNestedJoin = true;
			return currSize;
		}

		for(int i = 0; i < schema2.length; i++){
			schema1[currSize] = schema2[i];
			currSize++;
		}

		return currSize;
	}

	private boolean checkSelectAll(){
		if(isSelectAll)
			return true;
		if(selectColList.get(0) instanceof AllColumns){
			isSelectAll = true;
		}
		return isSelectAll;
	}


	private void createAllColumnList(int currSize){
		selectColList.clear();
		for(int i = 0; i < currSize; i++){
			selectColList.add(tempSchema[i]);
		}
	}


	private boolean creatAliasList(Column[] tempSchema, int currSize){
		tupleAliasList = new String[tempSchema.length];

		isSelectAll = checkSelectAll();

		if(isSelectAll){
			createAllColumnList(currSize);
			for(int i = 0; i < currSize; i++){
				tupleAliasList[i] = "";
			}
		}
		else{
			for(int i = 0; i < currSize; i++){
				for(int j = 0; j < selectColList.size(); j++){
					if(tempSchema[i].getWholeColumnName().equalsIgnoreCase(((SelectExpressionItem)selectColList.get(j)).getExpression().toString())
							|| tempSchema[i].getColumnName().equalsIgnoreCase(((SelectExpressionItem)selectColList.get(j)).getExpression().toString())){
						tupleAliasList[i] =  ((SelectExpressionItem)selectColList.get(j)).getAlias();
						if(tupleAliasList[i] == null){
							tupleAliasList[i] = "";
						}
					}
					else{
						tupleAliasList[i] = "";
					}
				}
			}
		}
		return isSelectAll;
	}
	
	private Operator getOperatorForSchema(Column[] schema, int operIndex){
		EvaluatorForSelectPushDown selectEval = new EvaluatorForSelectPushDown(schema);
		if(condition !=null){
			condition.accept(selectEval);
		}
		
		ArrayList<SelectPushDownBean> conditions = selectEval.conditions;
		if(conditions.size() > 0){
			//create index operator
			String indexName = selectEval.indexName;
			int colIndex = selectEval.colIndex;
			
			//create index operator
			SortedMap<Datum, ValueContainer> reducedMap;
			IndexOperator indexOper = new IndexOperator(UtilQE.getIndexPath(), indexName, input.get(operIndex).getColsDef(), colIndex);
			reducedMap = indexOper.index;
			for(SelectPushDownBean bean : conditions){
				String colName = bean.getColName();
				String condOper = bean.getCondition();
				Datum constVal = bean.getConstVal();
				if(condOper.equals("=")){
					reducedMap = new TreeMap<Datum, ValueContainer>();
					reducedMap.put(constVal, indexOper.index.get(constVal));
				}
				else if(condOper.equals(">")){
					reducedMap = reducedMap.tailMap(constVal);
					reducedMap.remove(constVal);
				}
				else if(condOper.equals(">=")){
					reducedMap = reducedMap.tailMap(constVal);
				}
				else if(condOper.equals("<")){
					reducedMap = reducedMap.headMap(constVal);
				}
				else if(condOper.equals("<=")){
					/*Map<Datum, ValueContainer> temp = reducedMap;
					constVal.incrementValue();
					SortedMap<Datum, ValueContainer> tempReduced = reducedMap.headMap(constVal);
					//temp.putAll(reducedMap.headMap(constVal));
					//ValueContainer val = temp.get(constVal);
					//tempReduced.put(constVal, val);
					reducedMap = (SortedMap<Datum, ValueContainer>) tempReduced;*/
					
					constVal.incrementValue();
					reducedMap = reducedMap.headMap(constVal);
					
				}
				else if(condOper.equals("<>")){
					reducedMap.remove(constVal);
				}
				
			}
			
			return new PushDownMapOperator(reducedMap);
		}
		else{
			//create scan operator
			Operator scanoper = input.get(operIndex);
			return scanoper;
		}
	}
	
	
	public ArrayList<Operator> sortJoinOperatorList(ArrayList<Operator> operlist){
		@SuppressWarnings("unchecked")
		ArrayList<Operator> operatorList = (ArrayList<Operator>) operlist.clone();
		HashMap<Operator,Integer> operatorToSchemaMapping = new HashMap<>();
		ArrayList<Column[]> primaryList = new ArrayList<Column[]>(), result = new ArrayList<Column[]>();
		int [] keyforJoinLocal = null;
		Iterator<Operator> it = operatorList.iterator();
		int i=0;
		for(;it.hasNext();){
			operatorToSchemaMapping.put(it.next(), i++);
		}
		Collections.sort(operatorList, new Comparator<Operator>() {
			@Override
			public int compare(Operator o1, Operator o2) {
				if(o1.getsize()>o2.getsize()){
					return 1;
				}else if(o1.getsize()<o2.getsize()){
					return -1;
				}else
					return 0;
			}
		});
		it = operatorList.iterator();
		i=0;
		for(Operator temp: operatorList){
			primaryList.add(schema.get(operatorToSchemaMapping.get(temp)));
			//operatorToSchemaMapping.put(temp, i++);
		}
		ArrayList<Operator> resultOperatorList = new ArrayList<Operator>();
		ArrayList<Operator> primaryOperatorList = new ArrayList<Operator>();
		ArrayList<Column[]> secondaryList =new ArrayList<Column[]>();
		ArrayList<Operator> secondaryOperatorList = new ArrayList<Operator>();
		primaryOperatorList = (ArrayList<Operator>) operatorList.clone();
		i=0;
		result.add(primaryList.remove(0));
		resultOperatorList.add(primaryOperatorList.remove(0));
		while(!primaryList.isEmpty()||!secondaryList.isEmpty()){
			int q = result.size();
			while(!primaryList.isEmpty()){
				for(int j=0;j<q;j++){
					EvaluatorForEquiJoinConditions equiEval = new EvaluatorForEquiJoinConditions(result.get(j), primaryList.get(0), true);
					if(condition !=null){
						condition.accept(equiEval);
					}
					keyforJoinLocal = equiEval.keyForEquiJoin;
					if(keyforJoinLocal[0]!=-1&&keyforJoinLocal[1]!=-1)
						break;
				}
				if(keyforJoinLocal[0]==-1||keyforJoinLocal[1]==-1){
					secondaryList.add(primaryList.remove(0));
					secondaryOperatorList.add(primaryOperatorList.remove(0));
				}else{
					result.add(primaryList.remove(0));
					resultOperatorList.add(primaryOperatorList.remove(0));
					secondaryList.addAll(primaryList);
					secondaryOperatorList.addAll(primaryOperatorList);
					break;
				}
			}
			primaryList.clear();
			primaryList.addAll(secondaryList);
			primaryOperatorList.clear();
			primaryOperatorList.addAll(secondaryOperatorList);
			secondaryList.clear();
			secondaryOperatorList.clear();

		}
		schema = result;
		return resultOperatorList;
	}

	@Override
	public Operator readAllTuples(boolean isOutermost) {
		List<ColumnDefinition> colsDef = null;
		Operator outputscan = null;
		String filename = "";

		long endTime;

		if(!isOutermost||orderbylist!=null){
			printToConsole = false;
		}
		else{
			printToConsole = true;
		}

		if(isOutermost){
			startTime = System.currentTimeMillis();
		}

		ArrayList<Datum> tuple = null;
		ArrayList<Datum> projectedTuple = null;
		int[] selectColNum = null;
		String[] selColMapGroup = null;
		TreeMap<String, ArrayList<Datum>> groupHashMap = null;
		TreeMap<String, Set<Datum>> groupDistinctHashMap = null;
		ArrayList<Integer> groupBySchema = null;

		boolean hasAvg = false;
		boolean isGroupBy = false;
		boolean joinFlag = false;
		ArrayList<Datum> tempGroupByTuple = null  ;

		int numOfOrderByCol;
		int numOfCols=0;
		boolean colPresent;
		ArrayList<Integer> orderByColPos=null;
		int[] ascFlag=null;
		boolean groupDistinct = false;
		/*String tableName;
		String indexes, splitIndex[];

		InputStream inputStream = null;
		try {

			inputStream = new FileInputStream(propertyFile);
			Set<String> keySet = tables.keySet();
			// load the properties file
			properties.load(inputStream);
			for(String key: keySet)
			{
				tableName = tables.get(key).getTable().getWholeTableName();
				//get the property value
				indexes =properties.getProperty("tableName");
				if(indexes!=null)
				{
					//Split the indexes obtained to obtain single indexes
					splitIndex = indexes.split(",");
					//Add every index for the corresponding table
					indexInfo.put(tableName, splitIndex);
				}
			}

		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}*/

		Operator tempOper = null;
		if (joinExists()){

			//tempOper = input.get(0);
			int tempSchemaSize = schema.get(0).length;

			//Add the first table in temp tuple

			//calculate the size for combined schema after join
			int schemaSize = 0;
			for(int i = 0; i < schema.size(); i++){
				schemaSize += schema.get(i).length;
			}
			
			input = sortJoinOperatorList(input);

			//add the first schema in temp schema
			tempSchema = new Column[schemaSize];
			int currentSize = 0;
			for(currentSize = 0; currentSize < schema.get(0).length; currentSize++){
				tempSchema[currentSize] = schema.get(0)[currentSize];
			}
			
			/*if(input.get(0) instanceof ScanOperator){
				input.set(0, getOperatorForSchema(schema.get(0), 0));
			}*/

			if (joinExists()){
				//call jointwotables till all tables are joined
				Operator oper1 = null;
				Operator oper2 = null;
				for(int index=1;index<schema.size();index++){
					//Join Schema
					Column [] schema1 = tempSchema.clone();
					currentSize = joinTwoSchemas(tempSchema, schema.get(index), currentSize);

					/////Create Alias List
					creatAliasList(tempSchema, currentSize);
					
					
					//Find the operator
				/*	if(input.get(index) instanceof ScanOperator){
						input.set(index, getOperatorForSchema(schema.get(index), index));
					}*/

					///-------------------------Join Table---------------------------//////////

					int indexForOper1 = -1;
					int indexForOper2 = -1;
					//first visit
					if(tempOper == null){
						//indexForOper1 = UtilQE.getOperatorForName(keysForIndex[0]);
						//indexForOper2 = UtilQE.getOperatorForName(keysForIndex[1]);
						startEndDateProcess(input.get(0),schema.get(0));
						oper1 = input.get(0);
						startEndDateProcess(input.get(index),schema.get(index));
						oper2 = input.get(index);
						
					}
					else{
						//indexForOper2 = UtilQE.getOperatorForName(keysForIndex[1]);
						oper1 = tempOper;
						startEndDateProcess(input.get(index),schema.get(index));
						oper2 = input.get(index);
						
					}

					//oper1 = tempOper;
					//oper2 = input.get(index);
					/*					boolean useIndexJoin = false;
					boolean isFirstIterOper = false;

					if(oper1 instanceof IndexOperator && !(oper2 instanceof IndexOperator)){
						String indexName = ((IndexOperator)oper1).getIndexName();
						if(indexName.equals(keysForIndex[0])){
							useIndexJoin = true;
						}
						isFirstIterOper = false; 
					}
					else if(oper2 instanceof IndexOperator && !(oper1 instanceof IndexOperator)){
						String indexName = ((IndexOperator)oper2).getIndexName();
						if(indexName.equals(keysForIndex[1])){
							useIndexJoin = true;
						}
						isFirstIterOper = true;
					}
					else if(oper1 instanceof IndexOperator && oper2 instanceof IndexOperator){
						int size1 = oper1.getsize();
						int size2 = oper2.getsize();
						if(size1 < size2){
							isFirstIterOper = true;
						}
						else{
							isFirstIterOper = false;
						}
						useIndexJoin = true;
					}
					else{
						int size1 = oper1.getsize();
						int size2 = oper2.getsize();
						if(size1 < size2){
							isFirstIterOper = true;
						}
						else{
							isFirstIterOper = false;
						}
						useIndexJoin = false;
					}*/

					/*if(useIndexJoin){
						//use index join
						System.out.println("using index join");
						tempOper = indexJoin(oper1, oper2, schema1, schema.get(index), isFirstIterOper);
						//tempOper = sortJoin(oper1, oper2, schema1, schema.get(index));						
					}
					else{*/
					//System.out.println("using hash join");
					if(index < schema.size() - 1){
						tempOper = joinTwoTablesBlockHash(oper1, oper2,schema1, schema.get(index), schema.get(index + 1));
					}
					else{
						tempOper = joinTwoTablesBlockHash(oper1, oper2,schema1, schema.get(index), null);
					}
					//System.out.println("returned");
					//}
					//joinCount++;
				}
			}
			joinFlag = true;
		}
		else
		{
			tempSchema = schema.get(0);
		}


		if(orderbylist!=null)
		{	orderByColPos = new ArrayList<Integer>(orderbylist.size());
		ascFlag =new int[orderbylist.size()];
		}	
		if(groupbylist != null){
			groupHashMap = new TreeMap<String, ArrayList<Datum>>();
			isGroupBy = true;
			groupDistinctHashMap = new TreeMap <String, Set<Datum>>();
			groupBySchema = new ArrayList<Integer>();
		}

		//////////////////FOR ORDER BY ///////////////////////////////////////////////
		int k=0;
		if(orderbylist!=null){
			if(checkSelectAll())
				numOfCols=tempSchema.length;
			else
				numOfCols = selectColList.size();
			for(OrderByElement order: orderbylist){
				colPresent = false;
				for(int i= 0; i<numOfCols;i++){

					if(checkSelectAll()) {

						if(tempSchema[i].toString().equalsIgnoreCase(order.getExpression().toString())){
							colPresent = true;
							orderByColPos.add(k,i);
							if(order.isAsc()) ascFlag[k]=1;
							else ascFlag[k]=-1;

							k++;
							break;
						}

					}
					else if( ((SelectExpressionItem)selectColList.get(i)).getExpression().toString().
							equalsIgnoreCase(order.getExpression().toString())|| 
							((((SelectExpressionItem)selectColList.get(i)).getAlias() != null)&&
									(((SelectExpressionItem)selectColList.get(i)).getAlias().toString().
											equalsIgnoreCase(order.getExpression().toString()))))
					{
						colPresent = true;
						orderByColPos.add(k,i);
						if(order.isAsc()) ascFlag[k]=1;
						else ascFlag[k]=-1;

						k++;
						break;
					}	
				}
				if(!colPresent)
				{
					SelectExpressionItem newItem = new SelectExpressionItem();
					newItem.setExpression(order.getExpression());
					selectColList.add(newItem);
					orderByColPos.add(k,selectColList.size()-1);
					if(order.isAsc())
						ascFlag[k]=1;
					else
						ascFlag[k]=-1;
					k++;

				}
			}
		}
		////////-----------------order by ends here----------//////////////////////////


		tupleAliasList = new String[tempSchema.length];
		//boolean isSelectAll = false;
		isSelectAll = creatAliasList(tempSchema, tempSchema.length);

		Evaluator eval = null;
		eval = new Evaluator ( tempSchema, null,tables,dataDir,groupHashMap, isGroupBy, tupleAliasList, selectColList,swapDir,groupDistinctHashMap,groupBySchema);
		boolean setMappingFlag = false;

		boolean multiSelect = false;
		if(joinFlag){
		}
		else{
			if(input.get(0) instanceof IndexOperator || input.get(0) instanceof ScanOperator){
				tempOper = input.get(0);

			}
			else{
				Operator scanOper = input.get(0).readAllTuples(false);

				tempOper = scanOper;
				tempOper.reset();
				multiSelect = true;
			}

		}	

		//System.out.println("joins done");
		projectedTuples = new ArrayList<ArrayList<Datum>>();
		while((tuple = tempOper.readOneTuple())!= null){
			if(tuple == null || tuple.size() == 0)
				break;
			eval.resetEval(tuple);
			eval.setSchemaLength(tempSchema.length);
			if(!joinFlag && condition !=null){
				condition.accept(eval);

				if(!eval.getBool()){
					continue;
				}
			}
			if(groupbylist != null){
				tempGroupByTuple = new ArrayList<Datum>();
				String groupbykey="";

				Datum value;
				////////////////////create key for hashmap ///////////////////////
				for(int i=0;i< groupbylist.size();i++){
					if(!(groupbylist.get(i) instanceof Function)){
						((Column)groupbylist.get(i)).accept(eval);
						value = eval.getAccumulator();
						groupbykey += value.toString();
					}
				}
				eval.setGroupKey(groupbykey);
				////////////////////////////////////////////////////////////////

				//check if the key is already present
				if(groupHashMap != null && groupHashMap.containsKey(groupbykey)){
					projectedTuple = groupHashMap.get(groupbykey);
					int count = ((DatumLong)projectedTuple.get(projectedTuple.size() - 1)).getValue().intValue();
					// The last item of projectedTuple carries count which is incremented everytime we find the same key
					projectedTuple.set(projectedTuple.size()-1,	 new Datum.DatumLong(count + 1));
					SelectExpressionItem s;
					for(int i =0;i< selectColList.size();i++){
						eval.setIndex(i);
						((SelectExpressionItem)selectColList.get(i)).getExpression().accept(eval);
						value = eval.getAccumulator();
					}
				}
				else{ 
					// if key is not already present. create a new entry in hashmap
					projectedTuple = new ArrayList<Datum>(selectColList.size());
					SelectExpressionItem s;
					for(int i =0;i< selectColList.size();i++){
						eval.setIndex(i);						
						((SelectExpressionItem)selectColList.get(i)).getExpression().accept(eval);
						value = eval.getAccumulator();
						projectedTuple.add(i, value);
					}
					projectedTuple.add(new Datum.DatumLong(1));
					groupHashMap.put(groupbykey, projectedTuple);
				}
				if(tempGroupByTuple.size() == 0)
					tempGroupByTuple = projectedTuple;
				/*if(useSwap){
					if(groupHashMap.size() >= HASH_MAP_LIMIT){
						boolean status = dumpToFile(groupHashMap);
						if(groupDistinctHashMap.size() > 0){
							groupDistinct = true;
							dumpDistinctToFile(groupDistinctHashMap);
							groupDistinctHashMap = new TreeMap<String,Set<Datum>>();
							eval.resetDistinctGroupHashMap(groupDistinctHashMap);
						}
						if(status)
							groupHashMap = new TreeMap<String, ArrayList<Datum>>();
						eval.resetGroupHashMap(groupHashMap);
					}	
				}*/
			}
			else{
				projectedTuple = new ArrayList<Datum>(selectColList.size());
				Datum value;

				for(int i =0;i< selectColList.size();i++){

					if(isSelectAll){
						((Column)selectColList.get(i)).accept(eval);
					}
					else{
						//check in schema if the column exist
						eval.setColumnIndex(i);
						eval.incrementCount();
						((SelectExpressionItem)selectColList.get(i)).getExpression().accept(eval);
					}


					value = eval.getAccumulator();
					projectedTuple.add(i, value);
				}
			}

			// create colsdef list for scan operator if not already created
			if(!isColsDefCreated){
				isColsDefCreated = true;
				colsDef = createColDef(projectedTuple);
				outputscan = new InMemoryOperator(projectedTuples);
			}

			if(groupbylist == null && !Evaluator.getIsFunction()){
				printTuple(projectedTuple);
			}

			if(!setMappingFlag){
				eval.setMappingFlag();
				setMappingFlag = true;
			}
		}

		/*if(useSwap){
			if(groupHashMap != null && groupHashMap.size() > 0){				
				boolean status = dumpToFile(groupHashMap);
				if (status){
					groupHashMap = new TreeMap<String, ArrayList<Datum>>();
				}
			}
			if(groupDistinctHashMap!= null && groupDistinctHashMap.size() > 0){
				groupDistinct = true;				
				dumpDistinctToFile(groupDistinctHashMap);
			}
		}*/

		groupDistinctHashMap = null;

		if(groupbylist == null && Evaluator.getIsFunction()){
			printTuple(projectedTuple);
		}
		else if(groupbylist != null){
			//read data from files
			/*if(useSwap){
				//System.out.println("group merge start");

				ArrayList<ScanOperator> scanOperators =initializeFilesReading((ArrayList<ColumnDefinition>) getGroupColDefs(tempGroupByTuple));
				ArrayList<ArrayList<Datum>> tuplesFromFiles = new ArrayList<ArrayList<Datum>>();
				groupBySchema = getGroupBySchema(eval.getGroupBySchema(),tempGroupByTuple.size());
				ArrayList<ColumnDefinition> groupByDatatypes = (ArrayList<ColumnDefinition>) getGroupColDefs(tempGroupByTuple);
				ArrayList<ArrayList<String>> distinctTuples = null;
				ArrayList<BufferedReader> bufferedReaders = null;
				if(groupDistinct){
					distinctTuples = new ArrayList<ArrayList<String>>();
					bufferedReaders = initializeDistinctFilesReading();
					bufferedReaders = getDistinctValuesFromFiles(bufferedReaders,distinctTuples);
				}
				for(int j=0;j<scanOperators.size();j++){
					tuplesFromFiles.add(scanOperators.get(j).readOneTuple());
				}
				while(true){
					ArrayList<Integer> minIndex = getMinIndices(tuplesFromFiles);
					if(minIndex == null)
						break;
					ArrayList<Datum> mergedKey = mergeGroupByKeys(tuplesFromFiles,groupByDatatypes,groupBySchema,minIndex,distinctTuples);
					for(int j =0;j<minIndex.size();j++){
						ArrayList<Datum> tempTuple = null;
						tempTuple = scanOperators.get(minIndex.get(j)).readOneTuple();
						tuplesFromFiles.set(minIndex.get(j),tempTuple);	
						if(groupDistinct){
							distinctTuples.set(minIndex.get(j), getDistinctTuple(bufferedReaders,minIndex.get(j)));
						}
					}
					mergedKey.remove(mergedKey.size()-1);					
					mergedKey.remove(0);
					printTuple(mergedKey);
				}
				if(groupDistinct){
					for(int i=0;i<bufferedReaders.size();i++){
						try {
							bufferedReaders.get(i).close();
						}catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
				for(int i =0;i<scanOperators.size();i++){
					scanOperators.get(i).reset();
				}
				//System.out.println("group merge after");				
			}*/
			//else{
			Collection<ArrayList<Datum>> coll = groupHashMap.values();
			Iterator<ArrayList<Datum>> i = coll.iterator();
			ArrayList<Datum> list = null;
			for(;i.hasNext();){
				list = i.next();
				list.remove(list.size()-1);
				//printTuple(pw, projectedTuple);
				printTuple(list);
			}
			//}					
		}
		if(orderbylist!=null) {
			outputscan=orderBy(outputscan, isOutermost, orderByColPos, numOfCols, ascFlag);
		}
		if(isOutermost){
			endTime = System.currentTimeMillis();
		}		
		return outputscan;
	}

	private void startEndDateProcess(Operator operator, Column[] schema) {
		ScanOperator scanOper = (ScanOperator) operator;
		String tableName = scanOper.tableName;
		if(tableName.equals("LINEITEM")|| tableName.equals("ORDERS")){
			EvaluatorForSpecificCondition eval = new EvaluatorForSpecificCondition(schema); 
			condition.accept(eval);
			if(eval.dates != null){
				scanOper.setStartEndDate(eval.dates[0], eval.dates[1]);
			}
		}		
	}

	private ArrayList<BufferedReader> initializeDistinctFilesReading() {
		// TODO Auto-generated method stub
		ArrayList<BufferedReader> bufferedReaders = new ArrayList<BufferedReader>();
		BufferedReader br = null;
		for(int i=0;i<groupByFileCount;i++){
			String path = swapDir + "/tempDistinct" + i; 
			try {
				br = new BufferedReader(new FileReader(path));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			bufferedReaders.add(br);			
		}
		return bufferedReaders;
	}

	private ArrayList<String> getDistinctTuple(ArrayList<BufferedReader> b,int i){
		String t;
		ArrayList<String> tuple = null;
		try {
			t = b.get(i).readLine();
			if (t != null){
				String[] temp1 = t.split("\\|");						
				tuple = new ArrayList<String>(Arrays.asList(temp1)) ;
			}			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return tuple;
	}

	private void dumpDistinctToFile(TreeMap<String, Set<Datum>> groupDistinctHashMap){		
		try {
			String path = swapDir + "/tempDistinct" + (groupByFileCount-1);
			File file = new File(path);

			// if file does not exist, then create it
			if (!file.exists()) {
				file.createNewFile();
			}
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			Iterator i = groupDistinctHashMap.entrySet().iterator();
			while(i.hasNext()){
				Map.Entry entry = (Map.Entry)i.next();
				String line = (String) entry.getKey();
				HashSet temp = (HashSet) entry.getValue();				
				Iterator<Datum> iterator = temp.iterator();
				while(iterator.hasNext()) {
					String setElement = iterator.next().toString();
					line = line + "|" + setElement; 
				}						
				line = line + "\n";
				bw.write(line);
			}

			bw.flush();
			fw.close();
			bw.close();
			fw.close();
		}catch (IOException e) {
			e.printStackTrace();
		}
	}

	private ArrayList<Integer> getGroupBySchema(ArrayList<Integer> groupBySchema, int groupBySize){
		for(int i=groupBySchema.size();i<(groupBySize-1);i++){
			groupBySchema.add(99);
		}
		groupBySchema.add(0,0);		
		groupBySchema.add(2);
		return groupBySchema;
	}

	private List<ColumnDefinition> getGroupColDefs(ArrayList<Datum> tempGroupByTuple){
		DatumString temp = new DatumString("keyDataType");
		ArrayList<Datum> temp1 = new ArrayList<Datum>();
		temp1.add(temp);
		for(int i=0;i < tempGroupByTuple.size();i++){
			temp1.add(tempGroupByTuple.get(i));
		}		
		return createColDef(temp1);
	}

	private ArrayList<Datum> mergeGroupByKeys(ArrayList<ArrayList<Datum>> tuples,ArrayList<ColumnDefinition> dataTypes,ArrayList<Integer> schema,ArrayList<Integer> minIndex,ArrayList<ArrayList<String>> distinctTuples){
		if(minIndex.size() == 1)
			return tuples.get(minIndex.get(0));
		ArrayList<Datum> tempResult = null;
		HashSet set = null;
		HashSet completeSet = null;
		// sum = 1, count = 2, avg = 3, min = 4, max = 5,countDistinct = 6,key = 0,col = 99 
		for(int i=0;i < minIndex.size();i++){
			ArrayList<Datum> tuple = tuples.get(minIndex.get(i));
			if(tempResult == null){
				tempResult = tuple;
				if(distinctTuples != null){
					distinctTuples.get(minIndex.get(i)).remove(0);					
					completeSet =  new HashSet(distinctTuples.get(minIndex.get(i)));					
				}
			}else{
				for(int j=1 ;j < tuple.size();j++){
					if(dataTypes.get(j).getColDataType().getDataType().equalsIgnoreCase("long")){
						switch(schema.get(j)){
						case 1:
						case 2:
							long tempVal = ((DatumLong)tempResult.get(j)).getValue().longValue();
							tempResult.set(j, new Datum.DatumLong(tempVal + ((DatumLong)tuple.get(j)).getValue().longValue()));
							break;
						case 3:
							long count1 = ((DatumLong)tempResult.get(tempResult.size() -1)).getValue().intValue();
							long count2 = ((DatumLong)tuple.get(tuple.size() -1)).getValue().intValue();
							long sum1 = ((DatumLong)tempResult.get(j)).getValue().longValue()* count1;
							long sum2 = ((DatumLong)tuple.get(j)).getValue().longValue()* count2;						
							double avg = (sum1+sum2)/(count1+count2);
							tempResult.set(j, new Datum.DatumDouble(avg));									
							break;
						case 4:
							if((((DatumLong)tempResult.get(j)).getValue().longValue()) > ((DatumLong)tuple.get(j)).getValue().longValue())
								tempResult.set(j,tuple.get(j));
							break;
						case 5:
							if((((DatumLong)tempResult.get(j)).getValue().longValue()) < ((DatumLong)tuple.get(j)).getValue().longValue())
								tempResult.set(j,tuple.get(j));						
							break;
						case 6:
							distinctTuples.get(minIndex.get(i)).remove(0);					
							set =  new HashSet(distinctTuples.get(minIndex.get(i)));	
							completeSet.addAll(set);			
							tempResult.set(j,new Datum.DatumLong(completeSet.size()));
							break;
						default:

						}
					}else{
						switch(schema.get(j)){
						case 1:
							double tempVal = ((DatumLong)tempResult.get(j)).getValue().doubleValue();
							tempResult.set(j, new Datum.DatumDouble(tempVal + ((DatumDouble)tuple.get(j)).getValue().doubleValue()));
							break;
						case 3:
							long count1 = ((DatumLong)tempResult.get(tempResult.size() -1)).getValue().intValue();
							long count2 = ((DatumLong)tuple.get(tuple.size() -1)).getValue().intValue();
							double sum1 = ((DatumDouble)tempResult.get(j)).getValue().doubleValue()* count1;
							double sum2 = ((DatumDouble)tuple.get(j)).getValue().doubleValue()* count2;						
							double avg = (sum1+sum2)/(count1+count2);
							tempResult.set(j, new Datum.DatumDouble(avg));							
							break;
						case 4:
							if((((DatumLong)tempResult.get(j)).getValue().longValue()) > ((DatumLong)tuple.get(j)).getValue().longValue())
								tempResult.set(j,tuple.get(j));
							break;
						case 5:
							if((((DatumLong)tempResult.get(j)).getValue().longValue()) < ((DatumLong)tuple.get(j)).getValue().longValue())
								tempResult.set(j,tuple.get(j));						
							break;
						default:

						}
					}
				}
			}
		}
		return tempResult;
	}

	private List<ColumnDefinition> createColDef(ArrayList<Datum> projectedTuple){
		String col = "";
		List<ColumnDefinition> colDefs= new ArrayList<ColumnDefinition>();
		for(Datum datum : projectedTuple){
			if(datum instanceof DatumLong) col = "long";
			else if(datum instanceof DatumDouble) col = "double";
			else if(datum instanceof DatumDate) col = "date";
			else col = "string";

			ColDataType colDataType = new ColDataType();
			colDataType.setDataType(col);
			ColumnDefinition coldef = new ColumnDefinition();
			coldef.setColDataType(colDataType);
			colDefs.add(coldef);
		}
		return colDefs;
	}

	//public void printTuple(PrintWriter pw, ArrayList<Datum> row)
	public void printTuple(ArrayList<Datum> row){

		if(!printToConsole){
			projectedTuples.add(row);
			return;
		}

		Iterator<Datum> it = null;
		if(row != null){
			it=row.iterator();
			//while(row != null){

			for(;it.hasNext();)
			{
				System.out.print(it.next().toString());
				if(it.hasNext()){
					System.out.print("|");
				}
			}
			System.out.println("");
		}

		if(limitCount != -1){
			limitCount--;
			if(limitCount == 0){
				long endTime = System.currentTimeMillis();
				//System.out.println(("Time taken: " + (endTime - startTime)));
				System.exit(0);
			}
		}
	}

	public Operator orderBy(Operator oper,Boolean isOutermost, ArrayList<Integer> size,int numOfCols,int[] ascFlag){
		//oper.reset();
		ArrayList<ArrayList<Datum>> tempBlock = null;

		tempBlock = ((InMemoryOperator)oper).getTuplesInMem();
		Collections.sort(tempBlock, new DatumComparator(size, orderbylist, selectColList, ascFlag));
		int remove = selectColList.size()-numOfCols;
		if(remove != 0){
			for(ArrayList<Datum> tup:projectedTuples){
				for(int i=0;i<remove;i++){
					tup.remove(tup.size()-1-i);
				}
			}
		}
		if(isOutermost) {
			printToConsole = true;
			for(ArrayList<Datum> data: tempBlock) {
				printTuple(data);
			}
			long endTime = System.currentTimeMillis();
			//System.out.println(("Time taken: " + (endTime - startTime)));
			//System.out.println(("Time taken: " + (endTime - UtilQE.startTime)));
			oper = null;	
		}
		else {
			oper = new InMemoryOperator(tempBlock);
		}
		return oper;	
	}

	@Override
	public void reset() {
		for(int i=0;i< input.size();i++)
			input.get(i).reset();
	}

	@Override
	public void setFuncFlag() {
		this.isFunc=true;
	}

	@Override
	public void setGroupFlag() {
		//this.isGroup=true;
	}

	boolean dumpToFile(Map<String, ArrayList<Datum>> groupHashMap){
		try {
			String path = swapDir + "/temp" + groupByFileCount;
			File file = new File(path);

			// if file does not exist, then create it
			if (!file.exists()) {
				file.createNewFile();
			}
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			Iterator i = groupHashMap.entrySet().iterator();
			while(i.hasNext()){
				Map.Entry entry = (Map.Entry)i.next();
				String line = (String) entry.getKey();

				ArrayList<Datum> temp = (ArrayList<Datum>) entry.getValue();
				for(int j=0;j<temp.size();j++){
					line = line + "|" +temp.get(j) ;
				}
				line = line + "\n";
				bw.write(line);
			}
			bw.flush();
			bw.close(); 
			groupByFileCount++;			
		}catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/*ArrayList<ScanOperator> initializeFilesReading(ArrayList<ColumnDefinition> groupBySchemaDataTypes){
		try{
			//List<ColumnDefinition> colsDef = createColDef(tempTuple);
			ArrayList<ScanOperator> scanOperators= new ArrayList<ScanOperator>();
			//	ArrayList<BufferedReader> bufferedReaders= new ArrayList<BufferedReader>();
			for(int i=0;i<groupByFileCount;i++){
				File file = new File(swapDir, "/temp"+i);				
				ScanOperator s = new ScanOperator(file, groupBySchemaDataTypes);
				scanOperators.add(s);
			}
			return scanOperators;
		}catch (Exception e){//Catch exception if any
			System.err.println("Error: " + e.getMessage());
			return null;
		}
	}*/

	ArrayList<BufferedReader> getDistinctValuesFromFiles(ArrayList<BufferedReader> bufferedReaders,ArrayList<ArrayList<String>> distinctTuples){
		try{
			ArrayList<String> temp = new ArrayList<String>();
			String strLine;
			for(int i=0;i < groupByFileCount;i++){
				String t = bufferedReaders.get(i).readLine(); 
				String[] temp1 = t.split("\\|");						
				ArrayList<String> tuple = new ArrayList<String>(Arrays.asList(temp1)) ;
				//	ArrayList<Datum> t2= temp1[1];
				distinctTuples.add(tuple);
			}				
			return bufferedReaders;
		}catch (Exception e){//Catch exception if any
			System.err.println("Error: " + e.getMessage());
			return null;
		}		
	}

	public ArrayList<Integer> getMinIndices(ArrayList<ArrayList<Datum>> tuplesFromFiles){
		if (tuplesFromFiles == null)	
			return null;

		String min = "";

		ArrayList<Integer> minIndex= new ArrayList<Integer>();
		for(int i=0;i<tuplesFromFiles.size();i++){
			if(tuplesFromFiles.get(i) != null){
				min = tuplesFromFiles.get(i).get(0).toString();
				break;
			}			
		}

		for(int i=0;i<tuplesFromFiles.size();i++){
			String temp = null;
			if((tuplesFromFiles.get(i) != null)&&(temp = tuplesFromFiles.get(i).get(0).toString())!= null){
				if(temp.compareTo(min) < 0){
					min = temp;
				}
			}
		}	
		for(int i=0;i<tuplesFromFiles.size();i++){
			String temp = "";			
			if((tuplesFromFiles.get(i) != null)&&(temp = tuplesFromFiles.get(i).get(0).toString())!= null){
				if(temp.equals(min)){
					minIndex.add(i);
				}
			}
		}
		if(minIndex.size() == 0)
			return null;
		return minIndex;
	}

	@Override
	public List<ColumnDefinition> getColsDef() {
		return null;
	}

	@Override
	public int getAllTuplesSize() {
		// TODO Auto-generated method stub
		return 0;
	}
	public Operator sortJoin(Operator index1,Operator index2,Column [] schema1,Column [] schema2){
		ArrayList<Datum> tuple1 = index1.readOneTuple();
		ArrayList<Datum> tuple2 = index2.readOneTuple();
		ArrayList<ArrayList<Datum>> outputTuples = new ArrayList<ArrayList<Datum>>(); 
		int columnIndex1 = keysForJoin[0];
		int columnIndex2 = keysForJoin[1];
		while(tuple1!= null && tuple2!= null){	
			Datum key1 = tuple1.get(columnIndex1);
			Datum key2 = tuple2.get(columnIndex2);
			if(key1.equals(key2)){
				ArrayList<Datum> opTuple = new ArrayList<Datum>();
				opTuple.addAll(tuple1);
				opTuple.addAll(tuple2);
				outputTuples.add(opTuple);
				tuple1 = index1.readOneTuple();				
			}else if(key1.minorThan(key2)){
				tuple1 = index1.readOneTuple();				
			}else{
				tuple2 = index2.readOneTuple();			
			}
		}	
		Operator inMemOp = new InMemoryOperator(outputTuples);
		return inMemOp;
	}	
}