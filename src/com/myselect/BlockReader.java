import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class BlockReader {
	private int flag=0;
	private BufferedReader br;
	
	public void read(String path,Block[] block)
	{
		try
		{
			int i=0,i1,i2;String tableName,temp;
			tableName = path;
			
			while(flag==0)
			{
				block[i] = new Block();
				block[i].setTableName(tableName);
				block[i].setBlockNo(i);
				if((Cache.getBlocks(path, i))==null){
					readBlock(br,block[i]);
				}
				else{
					block[i]=Cache.getBlocks(path, i);
				}
				
				i++;
			}
			Cache.setBlocks(block);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void readBlock(BufferedReader br,Block b) throws IOException
	{
		CblkRdr reader=new CblkRdr();
		String s[] = new String[10];
		int i=0;
		String t=null;
		int numtuples=reader.CtotalLines(b.getTableName(),55);
		int numblocks=numtuples/10;
		if(b.getBlockNo()<=numblocks){
			t=reader.CblkReader(10, b.getBlockNo(), 55, b.getTableName());
			s=t.split("\n");
			b.setTuples(s);
		}
		else if(b.getBlockNo()==numblocks+1){
			t=reader.CblkReader(10, b.getBlockNo(), 55, b.getTableName());
			s=t.split("\n");
			b.setTuples(s);
			flag=1;
		}
		else{
			flag=1;
			return;		
		}
	}
}
