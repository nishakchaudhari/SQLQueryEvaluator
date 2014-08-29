package edu.buffalo.cse562;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.buffalo.cse562.Datum.DatumDouble;
import edu.buffalo.cse562.Datum.DatumLong;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SubSelect;

public class EvaluatorForConditions implements ExpressionVisitor{

	private String colName;
	private Column[] schema;
	private String tableName1;
	private String tableName2;
	private String argCol;
	private Table argTable;
	
	public EvaluatorForConditions(String tableName1, String tableName2) {
		this.tableName1 = tableName1;
		this.tableName2 = tableName2;
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
			((Expression)arg0.getParameters().getExpressions().get(0)).accept(this);
		}
		else
			((Expression)arg0.getParameters().getExpressions().get(0)).accept(this);
		
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
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
	}

	@Override
	public void visit(Division arg0) {
		// TODO Auto-generated method stub
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
	}

	@Override
	public void visit(Multiplication arg0) {
		// TODO Auto-generated method stub
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
	}

	@Override
	public void visit(Subtraction arg0) {
		// TODO Auto-generated method stub
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
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
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
	}

	@Override
	public void visit(GreaterThan arg0) {
		//	System.out.println("in greater than");
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		
	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
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
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
	}

	@Override
	public void visit(MinorThanEquals arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
	
	}

	@Override
	public void visit(NotEqualsTo arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
	}

	@Override
	public void visit(Column arg0) {
		argCol = arg0.getColumnName();
		argTable = arg0.getTable();
		//arg0.
		
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