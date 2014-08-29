package edu.buffalo.cse562;

import java.io.BufferedWriter;
import java.io.PrintStream;
import java.io.PrintWriter;

public class TupleWriter {
	
	private PrintWriter pw = null;
	private boolean isConsole;
	private BufferedWriter bw = null;
	
	public TupleWriter(BufferedWriter bw, boolean isConsole)
	{
		this.bw = bw;
		this.pw = new PrintWriter(this.bw, true);
		this.isConsole = isConsole;
		
	}
	
	public TupleWriter(PrintStream ps, boolean isConsole)
	{
		this.pw = new PrintWriter(ps, true);
		this.isConsole = isConsole;
		
	}
	
	public BufferedWriter getBufferedWriter()
	{
		return this.bw;
	}
	
	public PrintWriter getPrintWriter()
	{
		return this.pw;
	}
	
	public boolean getIsConsole()
	{
		return this.isConsole;
	}

}
