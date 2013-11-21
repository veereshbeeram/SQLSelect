import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Parser {
	private Executor ex;
	private Condition con = new Condition();
	private Result res;
	private Table tb = new Table();
	
	public void parse(String input) 
	{
		String innerSelect=null,innermostSelect=null;int inSecSel=0;
		
		if(input.indexOf("in")!=-1 && (inSecSel=input.indexOf(Clauses.SELECT, input.indexOf("in")))!=-1)   // if there are nested queries
		{
			if(input.indexOf(Clauses.IN, inSecSel)!=-1 && input.indexOf(Clauses.SELECT, input.indexOf(Clauses.IN, inSecSel))!=-1) // 3 levels
			{
				ex = new Executor();
				innermostSelect = processInnermostQuery(input);       // the innermost query is resolved first
				process(innermostSelect);                         	//execute innermost select & resolve subsequent one
				input = resolveMiddleSelect(input);					
				con.getConAttr().clear();
			}
			ex = new Executor();		
			innerSelect = processInnerQuery(input);			       // resolve inner query now       
			process(innerSelect);                                  // process inner query 
			input = resolveOuterSelect(input);
			con.getConAttr().clear();
		}
		ex = new Executor();				                  //A executor to execute each query
		process(input);                           
		display();								             // display the result	
	}
		
	public void process(String input) 
	{
		Validator v = new Validator(tb,con);
		List<Table> tables;
		String[] words,tableList;
		List<Result> results = new ArrayList<Result>();Result tempRes;
		
		words = input.split(" ");								
		tableList = words[3].split(",");
		if(tableList.length>1)						  // if join : different validate method
		{
			tables = v.validateJoin(input);
			for(Table t : tables)
			{
				tempRes = ex.executeSelect(t);
				results.add(tempRes);
				ex = new Executor();
			}	
			res = ex.executeJoin(tables,tb,results);
			return;
		}
		
		v.validate(input);                         // Validating the input string
		
		if(con.getConAttr().size()==0)                    // if no conditions do normal Select
			res = ex.executeSelect(tb);
		else
			res = ex.executeSelect(tb,con);           // else Select based on condition
		
		if(tb.getDistinctAttribute()!=null)           // if Distinct attribute exists: change the result
			res = ex.selectDistinct(tb);
		
		if(tb.getInAttribute()!=null)                 //if IN clause exists: change the result 
			res = ex.selectIn(tb); 
		
		if(tb.getCountAttributes().size() !=0)                
			res = ex.selectCount(tb);
        
        if(tb.getAvgAttributes().size()!=0)             
			res = ex.selectAvg(tb);

        if(tb.getSumAttributes().size()!=0)              
			res = ex.selectSum(tb);
                        
        if(tb.getMinAttributes().size()!=0)               
			res = ex.selectMin(tb);
                        
        if(tb.getMaxAttributes().size()!=0)               
			res = ex.selectMax(tb);
	}
	
	public void display()
	{
		String temp="";int i=0;List<Integer> keys = new ArrayList<Integer>();
		HashMap<Integer,List<String>> result = res.getRes();
		for(String a : tb.getReqAttr())           // required attributes index stored in keys
		{
			if(a==null)
				break;
			keys.add(tb.getHMap().get(a.toLowerCase()));
			System.out.print(a.toUpperCase()+"\t\t");          // required attribute names displayed
			temp += a+"\t\t";
		}
		System.out.println("");
		for(int x=0;x<=.5*temp.length();x++)
		System.out.print("-----");
		System.out.println("");
		for(int j=0;j<res.getTCount();j++)          // only required attributes displayed from the 'result'
		{
			for(i=0;i<keys.size();i++)
				System.out.print(result.get(keys.get(i)).get(j)+"\t\t");
			System.out.println("");
		}
		System.out.println("\n"+res.getTCount()+" rows fetched");
	}

	public String processInnerQuery(String input)
	{
		int inIndex = input.indexOf("in");
		int inSelIndex = input.indexOf(Clauses.SELECT,inIndex);
		String innerSelect = input.substring(inSelIndex, input.length()-1);
		return innerSelect;
	}

	public String resolveOuterSelect(String input)
	{
		HashMap<Integer,List<String>> tempRes = res.getRes();
		int inIndex = input.indexOf("in");
		int selIndex = input.indexOf(Clauses.SELECT,inIndex);
		input = input.substring(0, selIndex);
		int reqAttr = tb.getHMap().get(tb.getReqAttr()[0]);
		for(int j=0;j<res.getTCount();j++)          
			input+=""+tempRes.get(reqAttr).get(j)+",";
		input = input.substring(0, input.length()-1);
		input+=")";
		return input;
	}
	
	public String processInnermostQuery(String input)
	{
		int selIndex = input.lastIndexOf(Clauses.SELECT);
		String innermostSelect = input.substring(selIndex, input.length()-3);
		return innermostSelect;
	}

	public String resolveMiddleSelect(String input)
	{
		HashMap<Integer,List<String>> tempRes = res.getRes();
		int selIndex = input.lastIndexOf(Clauses.SELECT);
		input = input.substring(0, selIndex);
		int reqAttr = tb.getHMap().get(tb.getReqAttr()[0]);
		for(int j=0;j<res.getTCount();j++)          
			input+=""+tempRes.get(reqAttr).get(j)+",";
		input = input.substring(0, input.length()-1);
		input+="))";
		return input;
	}
}


