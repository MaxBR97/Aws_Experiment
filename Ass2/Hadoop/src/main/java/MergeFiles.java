import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MergeFiles {
    
    // Concatenates files from a given input local directory into a given local output file
    public static void main(String[] args) throws Exception {
        
    	Configuration conf = new Configuration();
    	FileSystem hdfs = FileSystem.get(conf);
    	FileSystem local = FileSystem.getLocal(conf);
    	
        String inDirname = args[0];
        String outFilename = args[1];
        
        //File outFile = new File(outFilename);
        Path outFile = new Path(outFilename);
        
        //OutputStream outstream = new FileOutputStream(outFile);
        OutputStream outstream = hdfs.create(outFile);
                        
        PrintWriter writer = new PrintWriter(outstream);
        
        //File inDir = new File(inDirname);
        Path inDir = new Path(inDirname);
        
        //for (File inFile : inDir.listFiles()) {
        for (FileStatus inFile : local.listStatus(inDir)) {
        	
            //InputStream instream = new FileInputStream(inFile);
        	InputStream instream = local.open(inFile.getPath());
            
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(instream));
            
            String line=null;
            while ((line = reader.readLine()) != null)
                writer.println(line);
            reader.close();
        }
        writer.close();
    }
}
