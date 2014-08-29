package edu.buffalo.cse562;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import net.sf.jsqlparser.schema.Column;

public class SplitFiles {
	
	public void splitFiles(Operator oper,String column, int colPos, File indexDir, String fname)
	{
		ArrayList <Datum> tuple = new ArrayList<Datum>();
		int[] ascFlag = {1};
		ArrayList<Integer> i = new ArrayList<Integer>(); 
		ArrayList<ArrayList<Datum>> inMem = new ArrayList<ArrayList<Datum>>();
		i.add(colPos);
		while((tuple = oper.readOneTuple()) != null){
			inMem.add(tuple);
		}
		//System.out.println("inMem len ="+inMem.size());
		Collections.sort(inMem,new DatumComparator(i,null,null,ascFlag));
		//System.out.println("inMem len after sorting ="+inMem.size());
		printToFile(inMem,indexDir,fname);
		
	}
	
	//public void printTuple(PrintWriter pw, ArrayList<Datum> row)
		public void printToFile(ArrayList<ArrayList<Datum>> inMem, File indexDir, String fname){
		
			File file = new File(indexDir,fname+".dbr.s");
			//System.out.println("The name of the file is: "+fname);
			BufferedWriter bw;
			PrintWriter pw = null;
			try {
				bw = new BufferedWriter(new FileWriter(file));
				pw = new PrintWriter(bw,true);
				
				Iterator<ArrayList<Datum>> mainIter = null;
				Iterator<Datum> it = null;
				ArrayList<Datum> row = null;
				if(inMem != null)
				{
					mainIter = inMem.iterator();
					for(;mainIter.hasNext();)
					{
						row = mainIter.next();
						//System.out.println("row: "+row);
						if(row != null){
							it=row.iterator();
							for(;it.hasNext();)
							{
								pw.print(it.next().toString());
								if(it.hasNext()){
									pw.print("|");
								}
							}
							pw.println("");
						}
					}
				}
				pw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
}
