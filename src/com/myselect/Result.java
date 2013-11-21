import java.util.HashMap;
import java.util.List;

public class Result {
	private HashMap<Integer,List<String>> res = new HashMap<Integer,List<String>>();
	private int tCount;
	
	public int getTCount() {
		return tCount;
	}
	public void setTCount(int count) {
		tCount = count;
	}
	public HashMap<Integer, List<String>> getRes() {
		return res;
	}
	public void setRes(HashMap<Integer, List<String>> res) {
		this.res = res;
	}
}
