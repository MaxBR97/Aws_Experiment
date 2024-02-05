
//Example HTML file:
// <!-- <!DOCTYPE html>
// <html>
// <head>
// <title>Page Title</title>
// </head>
// <body>

// <h1>This is a Heading</h1>
// <p>This is a paragraph.</p>

// </body>
// </html> -->

import java.io.IOException;
import java.util.*;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Output {

    private List<ProcessedReview> processedReviews;

    public Output() {
        processedReviews = new LinkedList();
    }

     public void writeOutputToJSONFile(String path) {
        JSONObject obj = new JSONObject();
		JSONArray procReviews = new JSONArray();
        Iterator<ProcessedReview> it = processedReviews.iterator();
        for(int i=0; i<processedReviews.size(); i++) {
            JSONObject entry = new JSONObject();
            ProcessedReview pr = it.next();
            entry.put("link",pr.getLink());
            entry.put("color",pr.getColor().name());
            JSONArray allEntities = new JSONArray();    
            allEntities.addAll(pr.getNamedEntities());
            entry.put("namedEntities", allEntities);
            entry.put("isSarcastic",pr.isSarcastic());
            procReviews.add(entry);
        }
        obj.put("processedEntries",procReviews);

		try {
			FileWriter file = new FileWriter(path);
			file.write(obj.toJSONString());
			file.flush();
			file.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
    }

    //This function recieves list of file paths with JSON content, 
    //and it assembles all of them to the destination file, separated with "\n" between two JSONs.
    public static void assembleFiles(String[] paths, String destinationFile) {
       //to do    
    }

    public static void writeOutputToHTMLFile(String fileName) {
        // try {
        //     FileWriter file = new FileWriter(fileName);
        //     file.write("<html>\n<head>\n</head>\n<body>\n");
        //     for (ProcessedReview pr : processedReviews) {
        //         String color = pr.getColor().name().toLowerCase();
        //         file.write("<div style=\"color: " + color + ";\">\n");
        //         file.write("<p>Link: <a href=\"" + pr.getLink() + "\">" + pr.getLink() + "</a></p>\n");
        //         file.write("<p>Named Entities: [" + String.join(", ", pr.getNamedEntities()) + "]</p>\n");
        //         file.write("<p>Sarcasm Detection: " + (pr.isSarcastic() ? "Sarcastic" : "Not Sarcastic") + "</p>\n");
        //         file.write("</div>\n");
        //     }

        //     file.write("</body>\n</html>");
        //     file.flush();
        //     file.close();
        // } catch (IOException e) {
        //     e.printStackTrace();
        // }
    }

    public void appendProcessedReview(ProcessedReview pr) {
        processedReviews.add(pr);
    }

    public void appendProcessedReviews(List<ProcessedReview> pr) {
        processedReviews.addAll(pr);
    }
    
}