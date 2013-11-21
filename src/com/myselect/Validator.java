import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Validator {
	private Table tb;
	private Condition con;
	private BufferedReader br;
	String path = "";
	
	public Validator(Table tb,Condition con)
	{
		this.tb = tb;
		this.con = con;
	}
	
	public void validate(String input)
	{
		String[] words,fileCols,temp=null,attributes=null,tableList,tp=null;HashMap<String,Integer> h;
		int flag=0;
		words = input.split(" ");
		tableList = words[3].split(",");
		if(tableList.length>1)
		{
			validateJoin(input);
			return;
		}
			
		if(!input.contains(":"))
			tb.setTableName(tableList[0]);		
		else
			for(String t : tableList)
			{
				temp = t.split(":");
				tb.setTableName(temp[0]);
				tb.setTableRef(temp[1]);
			}
	
			path+=tb.getTableName();   
			tb.setPath(path);
		
		try
		{
			CblkRdr reader=new CblkRdr();
			String fileColsRet = reader.CblkReader(10, 0, 55, path);
			String[] fileColsTemp = fileColsRet.split("#");
			fileCols = fileColsTemp[0].split(";");
			h = hashFileColumns(fileCols);
			tb.setHMap(h);
			
			if(!(words[0].equalsIgnoreCase(Clauses.SELECT)))
				throw new Exception("No clause like "+words[0]);
			if(!(words[2].equalsIgnoreCase(Clauses.FROM)))
				throw new Exception("No clause like "+words[2]);
			if((words.length>4)&&!(words[4].equalsIgnoreCase(Clauses.WHERE)))
				throw new MyException("No clause like "+words[4]);
			
			if(words[1].equals("*")){
				attributes = fileCols;
				tb.setReqAttr(attributes);
				flag=1;
			}
			
			
			
			else if(words[1].contains(Clauses.DISTINCT))
			{
				if(words[1].indexOf(Clauses.DISTINCT)!=0||words[1].indexOf(Clauses.DISTINCT, words[1].indexOf(Clauses.DISTINCT))==-1)
					throw new MyException("Syntax Error near distinct");
				//get the distinct attribute 
				int in1 = words[1].indexOf('(');	
				int in2 = words[1].indexOf(')');
				String s = words[1].substring(in1+1,in2);
				
				if(s.contains("."))
					s = resolveRef(s,temp);  
				
				if(tb.getHMap().get(s)==null)
					throw new MyException("Entered attribute not present in table");
					
				attributes = words[1].split(",");
				for(int j=0;j<attributes.length;j++)
				{
					if(attributes[j].contains(Clauses.DISTINCT))
						attributes[j] = s;
					else if(attributes[j].contains("."))
						attributes[j]=resolveRef(attributes[j],tp);
				}
				if(attributes.length==1)	
					tb.setDistinctAttribute(s);
				flag=1;
				tb.setReqAttr(attributes);
			}
			
			
			
			
			else{
				if(words[1].contains(Clauses.MAX))
             {
				//get the Max attribute
				int in1 = 0;
				int in2 = 0,j=0;
                String s,s1;
                attributes = words[1].split(",");
                if(!validateAggregate(attributes)){
                	throw new MyException("Error at  "+words[1]);
                }
                while(words[1].indexOf("max(", in2)!=-1)
                	{
                		in1=words[1].indexOf("max", in2);
                		in2=words[1].indexOf(")", in1);
                		s = words[1].substring(in1+4, in2);
                		//resolve ref
                		if(s.contains("."))
                		{
                			s=resolveRef(s,temp);
                		}
                		if(tb.getHMap().get(s)==null)
                			throw new MyException("Entered attribute not present in table");
                         attributes[j++]=s;
                         tb.getMaxAttributes().add(s);
                         tb.addReqAttribute(s);
                        // System.out.println(s);
                	}
                
                
                flag=1;
             	}
			
			
			
			
			if(words[1].contains(Clauses.MIN))
            {
				//get the Max attribute
				int in1 = 0;
				int in2 = 0,j=0;
               String s,s1;
               attributes = words[1].split(",");
               if(!validateAggregate(attributes)){
               	throw new MyException("Error at  "+words[1]);
               }
               while(words[1].indexOf("min(", in2)!=-1)
               	{
               		in1=words[1].indexOf("min", in2);
               		in2=words[1].indexOf(")", in1);
               		s = words[1].substring(in1+4, in2);
               		//resolve ref
               		if(s.contains("."))
               		{
               			s=resolveRef(s,temp);
               		}
               		if(tb.getHMap().get(s)==null)
               			throw new MyException("Entered attribute not present in table");
                        attributes[j++]=s;
                        tb.getMinAttributes().add(s);
                        tb.addReqAttribute(s);
                    }
               //tb.setReqAttr(attributes);   
               flag=1;
            	}
			
			
			
			
			if(words[1].contains(Clauses.SUM))
            {
				//get the Max attribute

				int in1 = 0;
				int in2 = 0,j=0;
               String s,s1;
               attributes = words[1].split(",");
               if(!validateAggregate(attributes)){
               	throw new MyException("Error at  "+words[1]);
               }
               while(words[1].indexOf("sum(", in2)!=-1)
               	{
               		in1=words[1].indexOf("sum", in2);
               		in2=words[1].indexOf(")", in1);
               		s = words[1].substring(in1+4, in2);
               		//resolve ref
               		if(s.contains("."))
               		{
               			s=resolveRef(s,temp);
               		}
               		if(tb.getHMap().get(s)==null)
               			throw new MyException("Entered attribute not present in table");
                        attributes[j++]=s;
                        tb.getSumAttributes().add(s);
                        tb.addReqAttribute(s);
                    }
               flag=1;
            	}
			
			
			
			
			if(words[1].contains(Clauses.AVG))
            {
				//get the Max attribute

				int in1 = 0;
				int in2 = 0,j=0;
               String s,s1;
               attributes = words[1].split(",");
               if(!validateAggregate(attributes)){
               	throw new MyException("Error at  "+words[1]);
               }
               while(words[1].indexOf("avg(", in2)!=-1)
               	{
               		in1=words[1].indexOf("avg", in2);
               		in2=words[1].indexOf(")", in1);
               		s = words[1].substring(in1+4, in2);
               		//resolve ref
               		if(s.contains("."))
               		{
               			s=resolveRef(s,temp);
               		}
               		if(tb.getHMap().get(s)==null)
               			throw new MyException("Entered attribute not present in table");
                        attributes[j++]=s;
                        tb.getAvgAttributes().add(s);
                        tb.addReqAttribute(s);
                    }
               //tb.setReqAttr(attributes);     
               flag=1;
            	}
			
			
			
			if(words[1].contains(Clauses.COUNT))
            {
				int in1 = 0;
				int in2 = 0,j=0;
               String s,s1;
               attributes = words[1].split(",");
               if(!validateAggregate(attributes)){
               	throw new MyException("Error at  "+words[1]);
               }
               while(words[1].indexOf("count(", in2)!=-1)
               	{
               		in1=words[1].indexOf("count", in2);
               		in2=words[1].indexOf(")", in1);
               		s = words[1].substring(in1+6, in2);
               		if(s.equals("*")){
               			s=fileCols[0];
               		}
               		//resolve ref
               		if(s.contains("."))
               		{
               			s=resolveRef(s,temp);
               		}
               		if(tb.getHMap().get(s)==null)
               			throw new MyException("Entered attribute not present in table");
                        attributes[j++]=s;
                        tb.getCountAttributes().add(s);
                        tb.addReqAttribute(s);
                    }
               //tb.setReqAttr(attributes);     
               flag=1;
            }
		}
			
			if(flag==0)
			{
				attributes = words[1].split(",");
				for(int j=0;j<attributes.length;j++)
				{
					if(attributes[j].contains("."))  
					    attributes[j] = resolveRef(attributes[j],tp);	
					if(tb.getHMap().get(attributes[j])==null)
						throw new MyException("Attribute "+attributes[j]+" not present in table");
				}
				tb.setReqAttr(attributes);
				
			}
		
			//
			if(words.length>6 && words[4].equalsIgnoreCase(Clauses.WHERE) && words[6].contains(Clauses.IN))
			{
				if(words[5].contains("."))
				{
					tb.setInAttribute(resolveRef(words[5],tp));
				     if(tb.getHMap().get(tb.getInAttribute())==null)
						throw new MyException("Entered attribute not present in table");
				}
				else
				{
				    tb.setInAttribute(words[5]);
				    if(tb.getHMap().get(words[5])==null)
						throw new MyException("Entered attribute not present in table");
				}
				
				int in1 = words[6].indexOf('(');
				int in2 = words[6].indexOf(')');
				tb.setInAttributeValues(words[6].substring(in1+1,in2).split(","));
			}
			else if(words.length>4 && words[4].equalsIgnoreCase(Clauses.WHERE))
			{
				if(words.length>6 && words[6].equalsIgnoreCase(Clauses.AND))
					con.setAndExists(true);
				else if(words.length>6 && words[6].equalsIgnoreCase(Clauses.OR))
					con.setOrExists(true);
				int z=5;
				while(z<=7)
				{
				if(words[z].contains("!="))
					con.getConOp().add("!=");
				else if(words[z].contains("<="))
					con.getConOp().add("<=");
				else if(words[z].contains(">="))
					con.getConOp().add(">=");
				else if(words[z].contains("<"))
					con.getConOp().add("<");
				else if(words[z].contains(">"))
					con.getConOp().add(">");
				else if(words[z].contains("="))
					con.getConOp().add("=");
				else 
					throw new MyException("Error in WHERE clause near "+words[z]);
				temp = words[z].split(con.getConOp().get((z-5)/2));	//get appropriate operator
				if(temp[0].contains("."))
					temp[0]=resolveRef(temp[0],tp);
					
				if(tb.getHMap().get(temp[0].toLowerCase())==null)
					throw new MyException("Attribute "+temp[0]+" not present in table");
				con.getConAttr().add(temp[0]);
				con.getConAttrValue().add(temp[1]);
				if(con.isAndExists() || con.isOrExists())z+=2;
				else
					z+=3;
				}
			}
		}
		
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
	}
	
	public List<Table> validateJoin(String input) 
	{
        List<Table> tables = new ArrayList<Table>();HashMap<String,Integer> h;
		String words[],tableList[],temp[],fileCols1=null,tmpAttr[],attributes[],tmpPath,t1[],conAttr[]; 
		Table tbl;String temp1[];
		
		words = input.split(" ");
		attributes = words[1].split(",");
		tableList = words[3].split(",");
		conAttr=words[5].split("=");
		
		try {
		for(String t : tableList)
		{
			temp = t.split(":");
			tmpPath=path+temp[0];
			CblkRdr reader=new CblkRdr();
			fileCols1 =  reader.CblkReader(10, 0, 55, tmpPath);
			tbl = new Table();
			tbl.setPath(tmpPath);
			tbl.setTableName(temp[0]);
			tbl.setTableRef(temp[1]);
			tmpAttr = fileCols1.split(";");  
			for(String ta : tmpAttr)
			{
				tbl.getAllAttributes().add(ta);
				tb.getAllAttributes().add(ta);
			}
			h = hashFileColumns(tmpAttr);
			tbl.setHMap(h);
			tables.add(tbl);
		}
		temp1 = new String[tb.getAllAttributes().size()];
		temp1 = tb.getAllAttributes().toArray(temp1);
		h = hashFileColumns(temp1);
		tb.setHMap(h);
		
		for(Table tab : tables)
		{
			t1=conAttr[0].split("\\.");
		if(t1[0].equals(tab.getTableRef()))
			tab.addReqAttribute(t1[1]);
		else 
		{
			t1=conAttr[1].split("\\.");
			if(t1[0].equals(tab.getTableRef()))
			tab.addReqAttribute(t1[1]);
		}
		tab.setJoinAttribute(conAttr[0].split("\\.")[1]);
		}
		tb.setJoinAttribute(conAttr[0].split("\\.")[1]);
		
		
		for(String a : attributes)
		{
			t1 = a.split("\\.");
			for(Table tab : tables)
			{
				if(a.equals("*"))
				{
					temp1=new String[tab.getAllAttributes().size()];
					temp1 = tab.getAllAttributes().toArray(temp1);
					tab.setReqAttr(temp1);
					for(String str : temp1)
						tb.addReqAttribute(str);
				}
				else if(tab.getTableRef().equals(t1[0]))
				{
					if(tab.getHMap().get(t1[1].toLowerCase())==-1)
				    	throw new MyException("Attribute "+t1[1]+"not present in DB");
					else
					{
						tab.addReqAttribute(t1[1]);
						tb.addReqAttribute(t1[1]);
					}
				}
			}
		}
		}
		catch(Exception e){
			System.out.println("here"+e);
			e.printStackTrace();
		}
		
		return tables;
	}
	
	public HashMap<String,Integer> hashFileColumns(String fileCols[])
	{
		int k=0;
		HashMap<String,Integer> h = new HashMap<String,Integer>();
		for(String s : fileCols)
			if(!h.containsKey(s.toLowerCase()))
				h.put(s.toLowerCase(), k++); 	
		return h;
	}
	
	public String resolveRef(String s,String temp[]) throws MyException
	{
		  temp = s.split("\\.");
		  s=temp[1];
		  if(!(temp[0].equals(tb.getTableRef())))
			 throw new MyException("Improper usage of reference variable");
		  return s;
	}
	public boolean validateAggregate(String[] attributes){
		
		for(String s : attributes){
			if(s.contains(Clauses.AVG)||s.contains(Clauses.MAX)||s.contains(Clauses.MIN)||s.contains(Clauses.COUNT)||s.contains(Clauses.SUM))
				continue;
			else
				return false;
		}
		
		return true;
	}
}
