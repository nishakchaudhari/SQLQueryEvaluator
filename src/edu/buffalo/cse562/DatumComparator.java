package edu.buffalo.cse562;

import java.util.Comparator;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

public class DatumComparator implements Comparator<ArrayList<Datum>>{
	
	private ArrayList<Integer> orderByColPos;
	private List<OrderByElement> orderbylist;
	private List<SelectExpressionItem> selectColList;
	int[] ascFlag;
 
    public DatumComparator(ArrayList<Integer> orderByColPos, List<OrderByElement> orderbylist, List<SelectExpressionItem> selectColList, int[] ascFlag) {
		this.orderByColPos = orderByColPos;
		this.orderbylist = orderbylist;
		this.selectColList = selectColList;
		this.ascFlag=ascFlag;
	}

	@Override
	public int compare(ArrayList<Datum> o1, ArrayList<Datum> o2) {
		int value = 0;
		int i = 0;
		while (value == 0 && i< this.orderByColPos.size()){
			
			value = (o1.get(this.orderByColPos.get(i)).compareTo(o2.get(this.orderByColPos.get(i))));
			value*=ascFlag[i];
			i++;
		}
		return value;		
		}
}