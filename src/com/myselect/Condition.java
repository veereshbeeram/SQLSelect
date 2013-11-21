import java.util.ArrayList;
import java.util.List;

public class Condition {
	private List<String> conAttr = new ArrayList<String>(); 
	private List<String> conAttrValue = new ArrayList<String>();
	private List<String> conOp = new ArrayList<String>();
	private boolean andExists;
	private boolean orExists;
	
	public List<String> getConAttr() {
		return conAttr;
	}

	public void setConAttr(List<String> conAttr) {
		this.conAttr = conAttr;
	}

	public List<String> getConAttrValue() {
		return conAttrValue;
	}

	public void setConAttrValue(List<String> conAttrValue) {
		this.conAttrValue = conAttrValue;
	}

	public List<String> getConOp() {
		return conOp;
	}

	public void setConOp(List<String> conOp) {
		this.conOp = conOp;
	}

	public boolean isAndExists() {
		return andExists;
	}

	public void setAndExists(boolean andExists) {
		this.andExists = andExists;
	}

	public boolean isOrExists() {
		return orExists;
	}

	public void setOrExists(boolean orExists) {
		this.orExists = orExists;
	}
}
