package edu.buffalo.cse562;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import jdbm.Serializer;
import jdbm.SerializerInput;
import jdbm.SerializerOutput;

public class DatumSerializer implements Serializer<ValueContainer> {
	private int count =0;
	List<ColumnDefinition> colsDef;
	public DatumSerializer(List <ColumnDefinition> colsDef) {
		this.colsDef=colsDef;
	}

	@Override
	public ValueContainer deserialize(SerializerInput in) throws IOException,
	ClassNotFoundException {
		ArrayList<ArrayList<Datum>> tuples = new ArrayList<ArrayList<Datum>>();
		int count = in.readInt();
		for(int i=0;i<count;i++){
			ArrayList<Datum> tuple= new ArrayList<Datum>();
			for(ColumnDefinition datumType: colsDef) {
				if(datumType.getColDataType().getDataType().equalsIgnoreCase("long")||
						datumType.getColDataType().getDataType().equalsIgnoreCase("int")||
						datumType.getColDataType().getDataType().equalsIgnoreCase("number")||
						datumType.getColDataType().getDataType().equalsIgnoreCase("integer")) {
					tuple.add(new Datum.DatumLong(in.readLong()));
				}else if(datumType.getColDataType().getDataType().equalsIgnoreCase("double")||
						datumType.getColDataType().getDataType().equalsIgnoreCase("float")||
						datumType.getColDataType().getDataType().equalsIgnoreCase("decimal")) {
					tuple.add(new Datum.DatumDouble(in.readDouble()));
				}else if(datumType.getColDataType().getDataType().equalsIgnoreCase("date")) {
					@SuppressWarnings("deprecation")
					Date date =new Date(in.readInt(), in.readInt(), in.readInt());
					tuple.add(new Datum.DatumDate(date));
				}else {
					tuple.add(new Datum.DatumString(in.readUTF()));
				}
			}
			tuples.add(tuple);
		}
		ValueContainer value = new ValueContainer(tuples);
		return value;
	}

	@SuppressWarnings("deprecation")
	@Override
	public void serialize(SerializerOutput out, ValueContainer value) throws IOException {
		out.writeInt(value.count);
		for(ArrayList<Datum> tuple: value.tuples){
			int index =-1;
			for(ColumnDefinition datumType: colsDef) {
				if(datumType.getColDataType().getDataType().equalsIgnoreCase("long")||
						datumType.getColDataType().getDataType().equalsIgnoreCase("int")||
						datumType.getColDataType().getDataType().equalsIgnoreCase("number")||
						datumType.getColDataType().getDataType().equalsIgnoreCase("integer")) {
					out.writeLong((long)tuple.get(++index).getValue());
				}else if(datumType.getColDataType().getDataType().equalsIgnoreCase("double")||
						datumType.getColDataType().getDataType().equalsIgnoreCase("float")||
						datumType.getColDataType().getDataType().equalsIgnoreCase("decimal")) {
					out.writeDouble((double)tuple.get(++index).getValue());
				}else if(datumType.getColDataType().getDataType().equalsIgnoreCase("date")) {
					index++;
					out.writeInt(((Date)(tuple.get(index).getValue())).getYear());
					out.writeInt(((Date)(tuple.get(index).getValue())).getMonth());
					out.writeInt(((Date)(tuple.get(index).getValue())).getDate());
				}else {
					out.writeUTF(tuple.get(++index).getValue().toString());
				}
			}
		}
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

}
