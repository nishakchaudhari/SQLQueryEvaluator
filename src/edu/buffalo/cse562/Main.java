package edu.buffalo.cse562;

import java.io.*;
import java.util.*;

import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.create.table.*;
import net.sf.jsqlparser.statement.select.*;

public class Main {
	
	public static File swapDir = null;
	public static File indexDir = null;

	public static void main(String[] args) {
//		long maxBytes = Runtime.getRuntime().maxMemory();
//		System.out.println("Max memory: " + maxBytes / 1024 / 1024 + "M");
		//long startTime = System.currentTimeMillis();
		UtilQE.startTime = System.currentTimeMillis();
		//System.out.println("IN QueryEvaluation");
		File dataDir = null;
		Boolean buildFlag = false;
		int i;
		ArrayList<File> SqlFiles = new ArrayList<File>();
		
		for(i=0;i<args.length;i++){
			//System.out.println(args[i] + ": "+i+" Argument");
			if(args[i].equals("--data"))
			{
				dataDir=new File(args[i+1]);
				i++;
			}
			else if(args[i].equals("--swap"))
			{
				swapDir = new File(args[i+1]);
				i++;
			}
			else if(args[i].equals("--build")){
				buildFlag = true;
				createPropertyFile();
			}
			else if(args[i].equals("--index")){
				indexDir = new File(args[i+1]);
				i++;
			}
			else{
				SqlFiles.add(new File(args[i]));
			}
		}
		for(File sql:SqlFiles){
			try {
				FileReader stream = new FileReader(sql);				
				CCJSqlParser parser = new CCJSqlParser(stream);
				StatementEval statementeval= new StatementEval(dataDir,swapDir,buildFlag,indexDir);
				Statement stmt;				
				while((stmt = parser.Statement())!= null){	
					 stmt.accept(statementeval); 
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch(ParseException e){
				e.printStackTrace();
			}
		}
	}
	public static void createPropertyFile(){
		Properties prop = null;
		OutputStream output = null;

		prop = new Properties();
		output = null;
		try {
			output = new FileOutputStream("config.properties");
			output.close();			
		} catch (IOException  e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}