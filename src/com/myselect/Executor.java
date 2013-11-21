import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

public class Executor {
	private int tCount;
	private Result res = new Result();;
	private Block[] block = new Block[100];
	
	public Result executeSelect(Table tb)
	{
		String cells[]=null,line;int i=0,j=0;
		BlockReader br = new BlockReader();
		br.read(tb.getPath(),block);
		
		for(String s : tb.getReqAttr())
		{
			if(s==null)
				break;
			res.getRes().put(tb.getHMap().get(s.toLowerCase()), new ArrayList<String>());
		}
		if(tb.getInAttribute()!=null)
			res.getRes().put(tb.getHMap().get(tb.getInAttribute().toLowerCase()), new ArrayList<String>());
		
		while(block[j]!=null)
		{
			for(int k=0;k<10;k++)
			{
			try{
				if(block[j].getTuples()[k]==null)
					break;
				}
			catch(Exception e){
				break;
				}
			line = block[j].getTuples()[k];
			String[] tempCells=line.split("#");
			cells=tempCells[0].split(";");
			for(String s : tb.getReqAttr())
			{
				if(s==null)
				break;
				if(!(s.equalsIgnoreCase((tb.getInAttribute()))) && !(s.equalsIgnoreCase((tb.getJoinAttribute()))))
				{
					//System.out.println(s);
					i = tb.getHMap().get(s.toLowerCase());
					res.getRes().get(i).add(cells[i]);
				}
			}
			if(tb.getInAttribute()!=null)
			{
				i = tb.getHMap().get(tb.getInAttribute().toLowerCase());
				res.getRes().get(i).add(cells[i]);
			}
			if(tb.getJoinAttribute()!=null)
			{
				i = tb.getHMap().get(tb.getJoinAttribute().toLowerCase());
				res.getRes().get(i).add(cells[i]);
			}
			tCount++;
			}
		j++;
		}
		res.setTCount(tCount);
		return res;
	}

	public Result executeSelect(Table tb,Condition con)
	{
		String cells[]=null,line;int i=0,j=0,val,bCount=0;int gotoNext=1;int print=0;
		BlockReader br = new BlockReader();
		
		br.read(tb.getPath(),block);
		for(String s : tb.getReqAttr())
			res.getRes().put(tb.getHMap().get(s.toLowerCase()), new ArrayList<String>());
		
		
		
		while(block[bCount]!=null)
		{
			for(int k=0;k<10;k++)
			{
				try{
					if(block[bCount].getTuples()[k]==null)
						break;
					}
				catch(Exception e){
					break;
					}
				gotoNext=1;print=0;
				line = block[bCount].getTuples()[k];
				String[] tempCells=line.split("#");
				cells=tempCells[0].split(";");
				
				for(int z=0;z<con.getConAttr().size();z++)
				{
					j = tb.getHMap().get(con.getConAttr().get(z));
					val = cells[j].compareTo(con.getConAttrValue().get(z));
					switch(con.getConOp().get(z).charAt(0))
					{
					case '=' :  if(con.isAndExists())
								{
									if(val!=0)
									{
										print=0;
										gotoNext=0;
									}
									else
									{
										print=1;
										continue;
									}
									break;
								}
								else if(con.isOrExists())
								{
									if(val==0)
									{
										print=1;
										gotoNext=0;
									}
									else
										continue;
									break;
								}
								else if(val==0) print=1; 
								break;
					case '<' :  if(con.isAndExists())
								{
									if(val < 0)
										{
											print=1;
											continue;
										}
									else
										{
										  print=0;
										  gotoNext=0;
										}
									break;
								}
							  else if(con.isOrExists())
								{
									if(val < 0)
									{
										print=1;
										gotoNext=0;
									}
									else
										continue;
									break;
								}
							  else if(con.getConOp().get(z).length()==1 && val<0)
								print=1;
							 else if(con.getConOp().get(z).length()==2 && val<=0)
								print=1;
								break;
					case '>' : if(con.isAndExists())
					            {
									if(val > 0)
										{
										   print=1;
										   continue;
										}
									else
										{
										   print=0;
										   gotoNext=0;
										}
									break;
					            }
							 else if(con.isOrExists())
								{
									if(val > 0)
										{
										  print=1;
										  gotoNext=0;
										}
									else
											continue;
									break;
								} 
							else if(con.getConOp().get(z).length()==1 && val>0)
								print=1;
							else if(con.getConOp().get(z).length()==2 && val>=0)
								print=1;
								break;
					case '!' : if(con.isAndExists())
		            				{
										if(val != 0)
										{
											print=1;
											continue;
										}
										else
										{
											print=0;
											gotoNext=0;
										}
										break;
		            				}
							else if(con.isOrExists())
									{
										if(val != 0)
										{
											print=1;
											gotoNext=0;
										}
										else
											continue;
										break;
									}  
							else if(val!=0) print=1; 
								break;
					}
					if(gotoNext==0)break;
				}
			if(print==1)
			{
				for(String s : tb.getReqAttr())
				{
					i = tb.getHMap().get(s.toLowerCase());
					res.getRes().get(i).add(cells[i]);
				}
				tCount++;					
			}
			}
			bCount++;
		}
		
		res.setTCount(tCount);
		return res;
	}

	public Result selectDistinct(Table tb)
	{
		int i = tb.getHMap().get(tb.getDistinctAttribute().toLowerCase());
	
		HashSet<String> h = new HashSet<String>();
		for(int j=0;j<tCount;j++)
			for(int l=0;l<res.getRes().get(i).size();l++)
				h.add(res.getRes().get(i).get(l));
	
		res.getRes().get(i).clear();
		Iterator<String> it = h.iterator();
		while(it.hasNext())
			res.getRes().get(i).add(it.next()); 
		
		res.setTCount(res.getRes().get(i).size());
		return res;
	}

	public Result selectIn(Table tb)
	{
		int i = tb.getHMap().get(tb.getInAttribute().toLowerCase());
		List<Integer> index = new ArrayList<Integer>();List<String> temp=new ArrayList<String>();
		Iterator<Integer> it;Iterator<String> its;
		
		for(String s : tb.getInAttributeValues())
		{
			for(int l=0;l<res.getRes().get(i).size();l++)
				if(res.getRes().get(i).get(l).equals(s))
					index.add(l);
		}
		for(String s:tb.getReqAttr())
		{
			it = index.iterator();
			while(it.hasNext())
				temp.add(res.getRes().get(tb.getHMap().get(s.toLowerCase())).get(it.next()));
			res.getRes().get(tb.getHMap().get(s.toLowerCase())).clear();
			its = temp.iterator();
			while(its.hasNext())
				res.getRes().get(tb.getHMap().get(s.toLowerCase())).add(its.next());
			temp.clear();
		}
		res.setTCount(index.size());
		return res;
	}
	
	public Result executeJoin(List<Table> tables, Table tb,List<Result> results)
	{
		List<String> l1,l2;
		Result finalRes=new Result();
		String a,b,str;int i,z=0,tCount=0;HashMap<String,Integer> h1,h2 ;
		
		for(String s : tb.getAllAttributes())
		{
			if(s==null)
				break;
			finalRes.getRes().put(tb.getHMap().get(s.toLowerCase()), new ArrayList<String>());
		}
		
		String jAttr = tb.getJoinAttribute();
		
		h1=tables.get(0).getHMap();
		h2=tables.get(1).getHMap();
		l1=results.get(0).getRes().get(h1.get(jAttr.toLowerCase()));
		l2=results.get(1).getRes().get(h2.get(jAttr.toLowerCase()));
			
		for(int x=0;x<l1.size();x++)
		{
			a=l1.get(x);
			for(int y=0;y<l2.size();y++)
			{
			b=l2.get(y);
			if(a.equals(b))
			{ 
				finalRes.getRes().get(tb.getHMap().get(tb.getJoinAttribute())).add(a);
				i=x;
				z=0;
                while(z<2)
                {
                for(String s: tables.get(z).getReqAttr())
                {
                	if(s==null)
                		break;
                	if(!s.equalsIgnoreCase(tables.get(z).getJoinAttribute()))
                    {
                		str =results.get(z).getRes().get(tables.get(z).getHMap().get(s.toLowerCase())).get(i);
                		finalRes.getRes().get(tb.getHMap().get(s.toLowerCase())).add(str);
                	}
                }
                z++;
                i=y;
                }
             tCount++;    
			 }
			 }
		}	
	finalRes.setTCount(tCount);	
	return finalRes;
	}

	
	public Result selectMax(Table tb)
	{
	    int z=0;int i;
	    while(z<tb.getMaxAttributes().size())
	    {
	     i = tb.getHMap().get(tb.getMaxAttributes().get(z).toLowerCase());
	     String max = res.getRes().get(i).get(0);
	            for(String m : res.getRes().get(i))
	            {
	                
	                if(max.compareTo(m)<0)
	                    max = m;
	            }
	  
	        List<String> l = new ArrayList<String>();l.add(max);

	        res.getRes().put(i,l);
		res.setTCount(1);
		z++;

	        
	    }
	    return res;
	}

	
	public Result selectMin(Table tb)
	{
		
	    int z=0;int i;
	    while(z<tb.getMinAttributes().size())
	    {
	     i = tb.getHMap().get(tb.getMinAttributes().get(z).toLowerCase());
	     String min = res.getRes().get(i).get(0);
	            for(String m : res.getRes().get(i))
	            {

	                if(min.compareTo(m)>0)
	                    min = m;
	            }

	        List<String> l = new ArrayList<String>();l.add(min);

	        res.getRes().put(i,l);
		res.setTCount(1);
		z++;


	    }
	    return res;
	}
	public Result selectSum(Table tb)
	{
		int z=0;int i;
	    while(z<tb.getSumAttributes().size())
	    {
	     i = tb.getHMap().get(tb.getSumAttributes().get(z).toLowerCase());
	     int sum = Integer.parseInt(res.getRes().get(i).get(0));
	     for(String m : res.getRes().get(i))
	            {
	                sum+=Integer.parseInt(m);
	            }

	            List<String> l = new ArrayList<String>();l.add(Integer.toString(sum));
	        res.getRes().put(i,l);
		res.setTCount(1);
		z++;


	    }
	    return res;
	}

	public Result selectAvg(Table tb)
	{
	    int z=0;int i;
	    while(z<tb.getAvgAttributes().size())
	    {
	     i = tb.getHMap().get(tb.getAvgAttributes().get(z).toLowerCase());
	     int sum = Integer.parseInt(res.getRes().get(i).get(0));
	            for(String m : res.getRes().get(i))
	            {
	                sum+=Integer.parseInt(m);
	            }
	  float avg;
	  avg=(float)sum/(float)tCount;
	            List<String> l = new ArrayList<String>();l.add(Float.toString(avg));
	        res.getRes().put(i,l);
		res.setTCount(1);
		z++;


	    }
	    return res;
	}
	public Result selectCount(Table tb)
	{
		int z=0;int i;
		int counter=0;
	    while(z<tb.getCountAttributes().size())
	    {
	    	counter=0;
	     i = tb.getHMap().get(tb.getCountAttributes().get(z).toLowerCase());
	     int sum = Integer.parseInt(res.getRes().get(i).get(0));
	            for(String m : res.getRes().get(i))
	            {
	                if(m.equals("null"))
	                	continue;
	                counter++;
	            }
	 
	            List<String> l = new ArrayList<String>();
	            l.add(Float.toString(counter));
	        res.getRes().put(i,l);
		res.setTCount(1);
		z++;
	    }
	    return res;

	}


}


