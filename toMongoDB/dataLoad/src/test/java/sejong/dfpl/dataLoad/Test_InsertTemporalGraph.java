package sejong.dfpl.dataLoad;

import java.io.IOException;

public class Test_InsertTemporalGraph {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		
		InsertTool tool=new InsertTool("CollegeMsg");
		
		tool.insertTemporalGraph("C:\\data\\graphData\\CollegeMsg.txt");
	}

}
