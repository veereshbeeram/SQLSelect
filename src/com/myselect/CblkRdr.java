
public class CblkRdr {
	public native String blkrdr(int blkSize,int blkNum,int tupleLen,String filename);
	public native int totallines(int tupleLen,String filename);
	public String CblkReader(int blkSize,int blkNum,int tupleLen,String filename){
		//System.out.println("blockReader::");
		String value;
		value=blkrdr(blkSize,blkNum,tupleLen,filename);
		return value;
	}
	
	public int CtotalLines(String filename,int tupleLen){
		int retval;
		retval=totallines(tupleLen,filename);
		//System.out.println("CtotalLines::"+filename+"::"+tupleLen);
		return retval;
	}
	static{
		System.loadLibrary("blkrdr");
	}

}
