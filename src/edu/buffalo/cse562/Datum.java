package edu.buffalo.cse562;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

// Holds the data for tuples
public interface Datum extends Comparable<Datum>,Serializable {

	public Object getValue();
	public boolean minorThan(Object val);
	public void incrementValue();
	//public  int compareTo(Datum val1);
	
	//Class definition for Long
	public static class DatumLong implements Datum{
		long value;	
		public DatumLong(String value){
			this.value = Long.parseLong(value);
		}
		public DatumLong(long value){
			this.value = value;
		}
		public String toString(){
			return Long.toString(value);
		}
		public Long getValue(){
			return this.value;
		}
		
		@Override
		public int hashCode() {
			return ((Long)this.value).hashCode();
		}
		
		public boolean equals(Object val){
			if(this.value==((DatumLong)val).value){
				return true;
			}
			return false;
		}
		@Override
		public boolean minorThan(Object val) {
			DatumLong placeHolder= new DatumLong(val.toString());
			if(this.value<placeHolder.value){
				return true;
			}
			return false;
		}

		
		@Override
		public int compareTo(Datum val1) {
			if(this.minorThan(val1)){
				return -1;
			}
			else if(this.equals(val1)){
				return 0;
			}
			else
				return 1;
		}
		
		public void incrementValue(){
			this.value += 1;
		}
		
	}
	
	//Class definition for Double
	public static class DatumDouble implements Datum{
		double value;
		public DatumDouble(String value){
			this.value=Double.parseDouble(value);
		}
		public DatumDouble(double value){
			this.value=value;
		}
		@Override
		public Double getValue() {
//			 DecimalFormat twoDForm = new DecimalFormat("#.##");
//			    return Double.valueOf(twoDForm.format(this.value));
			    return this.value;
		}

		@Override
		public boolean minorThan(Object val) {
			DatumDouble placeHolder= new DatumDouble(val.toString());
			if(this.value<placeHolder.value){
				return true;
			}
			return false;
		}
		
		public String toString(){
//			 DecimalFormat twoDForm = new DecimalFormat("#.##");
//			    return Double.valueOf(twoDForm.format(this.value)).toString();
			return Double.toString(value);
		}
		
		@Override
		public int hashCode() {
			return ((Double)this.value).hashCode();
		}
		
		public boolean equals(Object val){
			if(this.value==((DatumDouble)val).value){
				return true;
			}
			return false;
		}
		
		@Override
		public int compareTo(Datum val1) {
			//System.out.println("In compareTo of Double");
			if(this.minorThan(val1)){
				return -1;
			}
			else if(this.equals(val1)){
				return 0;
			}
			else
				return 1;
		}
		
		public void incrementValue(){
			this.value += 0.000000001;
		}
		
	}
	
	//Class definition for String
	public static class DatumString implements Datum{
		String value;
		
		public DatumString(String value){
			this.value = value;
		}
		
		public String toString(){
			return value;
		}

		@Override
		public String getValue() {
			// TODO Auto-generated method stub
			return this.value;
		}
		
		@Override
		public int hashCode() {
			return this.value.hashCode();
		}
		
		public boolean equals(Object val){	
			if(this.value.equals(val.toString())){
				return true;
			}
			return false;
			
		}
		
		@Override
		public boolean minorThan(Object val) {
			if(this.value.compareTo(val.toString())<0){
				return true;
			}
			else
				return false;
		}

		

		@Override
		public int compareTo(Datum val1) {
			//System.out.println("In compareTo of String");
			if(this.minorThan(val1)){
				return -1;
			}
			else if(this.equals(val1)){
				return 0;
			}
			else
				return 1;
		}

		@Override
		public void incrementValue() {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	//Class definition for Date type 
	public static class DatumDate implements Datum{
		Date date;
		static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		
		public DatumDate(String value){
			try {
				date = sdf.parse(value);
			} catch (ParseException e) {
				//System.out.println("Invalid Date passed in constructor from string");
			}
		}
		
		public DatumDate(Date date){
				this.date = date;
		}
		
		public String toString(){
			return(sdf.format(date));
			
		}

		@Override
		public Date getValue() {
			return this.date;
		}
		
		@Override
		public int hashCode() {
			return this.date.hashCode();
		}
		
		@Override
		public boolean equals(Object val){
			Date datePassed;
			String temp = val.toString();
			try {
				datePassed = sdf.parse(temp);
				if(this.date.equals(datePassed))
					return true;
			} catch (ParseException e) {	
				//System.out.println("Invalid Date passed in equals");
		}
			return false;
		}

		@Override
		public boolean minorThan(Object val) {
		Date datePassed;
		String temp = val.toString();
		try {
			datePassed = sdf.parse(temp);
			if(this.date.before(datePassed))
				return true;
		} catch (ParseException e) {
			//System.out.println("Invalid Date passed in equals");
		}
		return false;
		}

		

		@Override
		public int compareTo(Datum val1) {
			//System.out.println("In compareTo of Date");
			if(this.minorThan(val1)){
				return -1;
			}
			else if(this.equals(val1)){
				return 0;
			}
			else
				return 1;
		}
		
		public void incrementValue(){
			Calendar c = Calendar.getInstance();
			c.setTime(this.date);
			c.add(Calendar.DATE, 1);  // number of days to add
			String dateStr = sdf.format(c.getTime());  // dt is now the new date
			try {
				this.date = sdf.parse(dateStr);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}	
}
