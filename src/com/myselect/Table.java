
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class Table {
	private HashMap<String,Integer> hMap;
	private String path;
	private String [] reqAttr;
	private String distinctAttribute,inAttribute,joinAttribute;
	public List<String> getMaxAttributes() {
		return maxAttributes;
	}

	public void setMaxAttributes(List<String> maxAttributes) {
		this.maxAttributes = maxAttributes;
	}

	public List<String> getMinAttributes() {
		return minAttributes;
	}

	public void setMinAttributes(List<String> minAttributes) {
		this.minAttributes = minAttributes;
	}

	public List<String> getAvgAttributes() {
		return avgAttributes;
	}

	public void setAvgAttributes(List<String> avgAttributes) {
		this.avgAttributes = avgAttributes;
	}

	public List<String> getSumAttributes() {
		return sumAttributes;
	}

	public void setSumAttributes(List<String> sumAttributes) {
		this.sumAttributes = sumAttributes;
	}
	private List<String> maxAttributes = new ArrayList<String>();
    private List<String> minAttributes = new ArrayList<String>();
    private List<String> avgAttributes = new ArrayList<String>();
    private List<String> sumAttributes = new ArrayList<String>();
    private List<String> countAttributes = new ArrayList<String>();
	public List<String> getCountAttributes() {
		return countAttributes;
	}

	public void setCountAttributes(List<String> countAttributes) {
		this.countAttributes = countAttributes;
	}
	private String[] inAttributeValues;
	private String tableName;
	private String tableRef;
	private List<String> allAttributes = new ArrayList<String>();
	private int i;
	
	public String toString()
	{
		return tableName;
	}
	
	public HashMap<String, Integer> getHMap() {
		return hMap;
	}
	public void setHMap(HashMap<String, Integer> map) {
		hMap = map;
	}
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public String[] getReqAttr() {
		return reqAttr;
	}
	public void setReqAttr(String[] reqAttr) {
		this.reqAttr = reqAttr;
	}
	public String getDistinctAttribute() {
		return distinctAttribute;
	}
	public void setDistinctAttribute(String distinctAttribute) {
		this.distinctAttribute = distinctAttribute;
	}
	public String getInAttribute() {
		return inAttribute;
	}
	public void setInAttribute(String inAttribute) {
		this.inAttribute = inAttribute;
	}
	public String[] getInAttributeValues() {
		return inAttributeValues;
	}
	public void setInAttributeValues(String[] inAttributeValues) {
		this.inAttributeValues = inAttributeValues;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getTableRef() {
		return tableRef;
	}
	public void setTableRef(String tableRef) {
		this.tableRef = tableRef;
	}
	public List<String> getAllAttributes() {
		return allAttributes;
	}
	public void setAllAttributes(List<String> allAttributes) {
		this.allAttributes = allAttributes;
	}
	
	public void addReqAttribute(String a)
	{
	  if(reqAttr==null)
		  reqAttr= new String[20];
	  reqAttr[i++]=a;
	 }
	public String getJoinAttribute() {
		return joinAttribute;
	}
	public void setJoinAttribute(String joinAttribute) {
		this.joinAttribute = joinAttribute;
	}
	
}
