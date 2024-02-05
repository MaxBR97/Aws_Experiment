
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.json.simple.*;
import org.json.simple.parser.JSONParser;

public class Input {
    private String title;
    private List<Review> reviews;

    private Input()
    {}

    public static List<Input> parseFileToInputObjects(String path) {
        List<Input> ans = new LinkedList<Input>();

        File inputFile = new File(path);
        // File parentDir = currentDir.getParentFile();
        // File newFile = new File(parentDir,"Example.txt");;
        try{
            JSONParser parser = new JSONParser();
            Reader reader = new FileReader(inputFile);
            List<String> inputTextLines = Files.readAllLines(Paths.get(inputFile.getAbsolutePath()));
            for(String line: inputTextLines){
                Input cur = new Input();
                cur.reviews = new LinkedList<Review>();
                Object jsonObj = parser.parse(line);
                JSONObject jsonObject = (JSONObject) jsonObj;
                cur.title = (String) jsonObject.get("title");
                Iterator<JSONObject> it = ((JSONArray) jsonObject.get("reviews")).iterator();
                while (it.hasNext()) {
			        Review r = new Review(it.next());
                    cur.reviews.add(r);
		        }
                ans.add(cur);
            }
            reader.close();
        } catch(Exception e) {
            e.printStackTrace();
        }

        return ans;
    }

    //to do
    // parseFileToInputObjects returns all entries of JSON objects separated by \n, and returns these entries as List<Input>
    // writeInputObjectToFile writes the given Input object to file path, as a JSON.
    public static void writeInputObjectToFile(Input inp, String path){}

    public String getTitle() {
        return title;
    }

    public List<Review> getReviews() {
        return reviews;
    }

    public String toString() {
        return "Input{" +
                "title='" + title + '\'' +
                ", reviews=" + reviews +
                '}';
    }
}
