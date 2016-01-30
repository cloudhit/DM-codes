import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import org.apache.mahout.math.Matrices;
import org.apache.mahout.math.Matrix;

public class GaussianMatrix
{
	public static void main(String[] args) throws IOException
	{
		Matrices matrix = new Matrices();
		int seed = 0;
		Random random = new Random(seed);
		
		Matrix matrixView = matrix.gaussianView(Reformat.userNum, Reformat.k, seed);
		File file = new File("GaussianMatrix.txt");
		BufferedWriter wr = new BufferedWriter(new FileWriter(file));
		for(int i = 0; i < Reformat.userNum; i ++)
		{
			for(int j = 0; j < Reformat.k; j ++)
			{
				double ele = matrixView.get(i, j);
				wr.write("O," + (i+1) + "," + (j+1) + "\t" + ele + "\n");
			}
		}
		wr.close();
	}
}
