public class Block {
	
	private String[] tuples;
	private int blockNo;
	private String tableName;
	
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public int getBlockNo() {
		return blockNo;
	}
	public void setBlockNo(int blockNo) {
		this.blockNo = blockNo;
	}
	public String[] getTuples() {
		return tuples;
	}
	public void setTuples(String[] tuples) {
		this.tuples = tuples;
	}
}
