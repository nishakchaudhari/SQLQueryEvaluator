package edu.buffalo.cse562;

import java.util.ArrayList;

public class DatumList {
	ArrayList<Datum[]> list;
	 public void add(Datum value[]){
		 list.add(value);
	 }
	 public void print(){
		 for(Datum[] dat: list){
			 for(int i=0;i<dat.length;i++){
				 System.out.println(dat[i].getValue());
			 }
		 }
	 }
}
