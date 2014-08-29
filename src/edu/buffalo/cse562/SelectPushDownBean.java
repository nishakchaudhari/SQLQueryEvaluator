package edu.buffalo.cse562;

public class SelectPushDownBean {
	
	private String condition;
	private String colName;
	private Datum constVal;
	
	
	public SelectPushDownBean(String condition, String colName, Datum constVal) {
		super();
		this.condition = condition;
		this.colName = colName;
		this.constVal = constVal;
	}


	public String getCondition() {
		return condition;
	}


	public void setCondition(String condition) {
		this.condition = condition;
	}


	public String getColName() {
		return colName;
	}


	public void setColName(String colName) {
		this.colName = colName;
	}


	public Datum getConstVal() {
		return constVal;
	}


	public void setConstVal(Datum constVal) {
		this.constVal = constVal;
	}
	

}
