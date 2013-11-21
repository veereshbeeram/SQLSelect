import java.io.*;

public class Tester {

	/**
	 * @param args
	 */
	
	public static void main(String[] args) throws IOException,MyException{
		// TODO Auto-generated method stub
		
		new Tester().run();
	}
	
	public void run() throws IOException,MyException
	{
		String input="";Parser p;
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		while(true)
		{
		System.out.print("sql> ");
		input = br.readLine();
		input = input.trim().toLowerCase();
		if(input.equals("e"))
			break;
		if(input.equals(""))
			continue;
		p = new Parser();
		p.parse(input);
		}
	}

}


