public class Cache {
	private static int noOfBlocks = 10;
	private static Block[] blocks=new Block[10];

	public static Block getBlocks(String tablename, int BlockNum) {
		Block temp=null;
		for(int i=0;i<noOfBlocks;i++){
			if(blocks[i]==null){
				continue;
			}
			if(blocks[i].getTableName().equals(tablename)){
				if(blocks[i].getBlockNo()==BlockNum )
					return blocks[i];
			}
		}
		return temp;
	}
	public static void setBlocks(Block[] b) {
		blocks = b;
	}
}
