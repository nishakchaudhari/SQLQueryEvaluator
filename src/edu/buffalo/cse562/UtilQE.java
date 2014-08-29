package edu.buffalo.cse562;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;

import net.sf.jsqlparser.statement.create.table.CreateTable;

public class UtilQE {
	
	private static File propertyFile =new File("config.properties");
	private static Properties properties = new Properties();
	private static HashMap<String,String[]> indexInfo;
	private static boolean isIndexInfoDone = false;
	private static String indexPath;
	private static HashMap<String, Integer> tableOperMap = new HashMap<String, Integer>();
	public static long startTime;

	public static void createIndexInfo(HashMap<String, CreateTable> tables, String filepath){
		indexPath = filepath;
		if(!isIndexInfoDone){
			indexInfo = new HashMap<String,String[]>();
			String tableName;
			String indexes, splitIndex[];

			InputStream inputStream = null;
			try {

				inputStream = new FileInputStream(propertyFile);
				Set<String> keySet = tables.keySet();
				// load the properties file
				properties.load(inputStream);
				for(String key: keySet)
				{
					tableName = tables.get(key).getTable().getWholeTableName();
					//get the property value
					indexes =properties.getProperty(tableName);
					if(indexes!=null)
					{
						//Split the indexes obtained to obtain single indexes
						splitIndex = indexes.split(",");
						//Add every index for the corresponding table
						indexInfo.put(tableName, splitIndex);
					}
				}

			} catch (IOException ex) {
				ex.printStackTrace();
			} finally {
				if (inputStream != null) {
					try {
						inputStream.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
		
	}
	
	public static HashMap<String,String[]> getIndexInfo(){
		return indexInfo;
	}
	
	public static String[] getColumnsForTableName(String tablename){
		return indexInfo.get(tablename);
	}
	
	public static String getIndexPath(){
		return indexPath;
	}
	
	public static void addOperToIndexOperMap(String indexFileName, int indexInOperList){
		tableOperMap.put(indexFileName, indexInOperList);
	}
	
	public static int getOperatorForName(String tableName){
		//System.out.println("Get operator for table name:" + tableName );
		return tableOperMap.get(tableName);
	}
}
