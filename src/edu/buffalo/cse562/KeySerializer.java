package edu.buffalo.cse562;

import java.io.IOException;
import java.util.Date;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import jdbm.Serializer;
import jdbm.SerializerInput;
import jdbm.SerializerOutput;

public class KeySerializer implements Serializer<Datum> {
	ColumnDefinition type;
	public KeySerializer(ColumnDefinition type) {
		this.type = type;
	}
	
	@SuppressWarnings("deprecation")
	@Override
	public Datum deserialize(SerializerInput in) throws IOException,
			ClassNotFoundException {
		Datum key ;
		if(type.getColDataType().getDataType().equalsIgnoreCase("long")||
				type.getColDataType().getDataType().equalsIgnoreCase("int")||
				type.getColDataType().getDataType().equalsIgnoreCase("number")||
				type.getColDataType().getDataType().equalsIgnoreCase("integer")) {
			key = new Datum.DatumLong(in.readLong());
		}else if(type.getColDataType().getDataType().equalsIgnoreCase("double")||
					type.getColDataType().getDataType().equalsIgnoreCase("float")||
					type.getColDataType().getDataType().equalsIgnoreCase("decimal")) {
			key = new Datum.DatumDouble(in.readDouble());
		}else if(type.getColDataType().getDataType().equalsIgnoreCase("date")) {
			key = new Datum.DatumDate(new Date(in.readInt(),in.readInt(),in.readInt()));
		}else {
			key = new Datum.DatumString(in.readUTF());
		}
		return key;
	}

	@Override
	public void serialize(SerializerOutput out, Datum key) throws IOException {
		if(type.getColDataType().getDataType().equalsIgnoreCase("long")||
				type.getColDataType().getDataType().equalsIgnoreCase("int")||
				type.getColDataType().getDataType().equalsIgnoreCase("number")||
				type.getColDataType().getDataType().equalsIgnoreCase("integer")) {
			out.writeLong((long)key.getValue());
		}else if(type.getColDataType().getDataType().equalsIgnoreCase("double")||
					type.getColDataType().getDataType().equalsIgnoreCase("float")||
					type.getColDataType().getDataType().equalsIgnoreCase("decimal")) {
			out.writeDouble((double)key.getValue());
		}else if(type.getColDataType().getDataType().equalsIgnoreCase("date")) {
			out.writeInt(((Date)(key.getValue())).getYear());
			out.writeInt(((Date)(key.getValue())).getMonth());
			out.writeInt(((Date)(key.getValue())).getDate());
		}else {
			out.writeUTF(key.getValue().toString());
		}
	}

}
