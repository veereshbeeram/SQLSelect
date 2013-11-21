
public class MyException extends Exception{

public MyException(String mes)
{
	System.out.println(mes);
	try{
	new Tester().run();
	}
	catch(Exception e){
		System.out.println("hi");
	}
	
}
}
