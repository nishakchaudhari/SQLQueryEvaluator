package edu.buffalo.cse562;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.Union;


public class SelectEval implements SelectVisitor {
	
	File dataDir;
	File swapDir;
	HashMap<String, CreateTable> tables;
	Operator oper;
	List selectColList;
	boolean func=false;
	boolean groupby=false;
	long limitCount = -1;
	File indexDir ;
	
	public SelectEval(File dataDir,HashMap<String, CreateTable> tables,File swapDir,File indexDir) {
		this.dataDir=dataDir;
		this.tables = tables;
		this.swapDir = swapDir;
		this.indexDir = indexDir;
	}

	@Override
	public void visit(PlainSelect plainselect) {
		
		selectColList = plainselect.getSelectItems();
		//System.out.println("selectColList: "+selectColList.toString());
		FromScanner fromscanner = new FromScanner(dataDir,tables, selectColList, swapDir,indexDir);
		plainselect.getFromItem().accept(fromscanner);
		
		if (plainselect.getJoins() != null) {
			for (Iterator joinsIt = plainselect.getJoins().iterator(); joinsIt.hasNext();) {
				Join join = (Join) joinsIt.next();
				join.getRightItem().accept(fromscanner);
			}
		}	

		if(plainselect.getLimit()!=null)
		{
			Limit limit = plainselect.getLimit();
			limitCount = limit.getRowCount();
		}
		
		if(plainselect.getGroupByColumnReferences() != null){
			this.groupby=true;
		}
		ArrayList<Operator> oper1 = fromscanner.source;
		SelectOperator operator = null;
		
		if(plainselect.getWhere()!=null)
		{
			oper = new SelectOperator(oper1, fromscanner.schema, selectColList, plainselect.getWhere(),tables,dataDir,plainselect.getGroupByColumnReferences(),plainselect.getOrderByElements(),swapDir,limitCount);
				
		}
		else
		{
			oper = new SelectOperator(oper1, fromscanner.schema, selectColList, null,tables,dataDir, plainselect.getGroupByColumnReferences(), plainselect.getOrderByElements(),swapDir,limitCount);
		}		
		
	}

	@Override
	public void visit(Union arg0) {		
		
	}	
}