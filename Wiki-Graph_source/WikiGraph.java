import org.graphstream.graph.*;
import org.graphstream.graph.implementations.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Scanner;

public class WikiGraph {
	static HashMap<String,String> isbn_to_title; 
	
public static void main(String args[]) throws IOException{
	readMapFromFile("isbnBooktitleMap"); //load HashMap from file if possible
	Graph graph = new SingleGraph("Wikipedia");
	//graph.setAutoCreate(true);
    graph.setStrict(false);
	System.setProperty("org.graphstream.ui", "swing");
	// we do some styling to the nodes and edges using css
	graph.setAttribute("ui.stylesheet", 
            "node.marked {" +
            "	fill-color: red;" +
            "}"
            + "edge {" +
            "	fill-color: rgba(0,0,0,200);" +
            "	size: 1px;" +
            "}"+"graph {fill-color: white; fill-mode: plain;}"
            );
	
	graph.setAttribute("ui.title", "Wikipedia ISBN");
	//graph.setAttribute("ui.stylesheet","canvas-color: black;");
	
	try(BufferedReader in = new BufferedReader(new FileReader("graph"))){
		String line;
	        while((line = in.readLine()) != null){
	            String[] pair = line.split("\t", 2);
	            //System.out.println(line);
	            if(pair.length==2) {
	            	Node isbn = graph.addNode(pair[0]);
	            	Node article = graph.addNode(pair[1]);
			        article.setAttribute("ui.style", "fill-color: rgba(200,120,74,200);");
			        //System.out.println("nodes~>" +pair[0] +" "+pair[1]);
			       //make graph
			        graph.addEdge(""+ pair[0] + pair[1] +"", pair[0], pair[1]);
			        int px = (5 + isbn.getDegree() / 20);
			        article.setAttribute("ui.style", "size: 5px;");
			        isbn.setAttribute("ui.style", "size:" + Integer.toString(px) +"px;");
			        isbn.setAttribute("ui.style", "fill-color: rgb(200,0,0);");
			        if(isbn.getDegree() > 4) { 
			        	//Only label isbn's that occur in 5 or more articles
			        	isbn.setAttribute("ui.label", getWikiName(pair[0]));
			        	isbn.setAttribute("ui.style", "text-color: black;");
			        	isbn.setAttribute("ui.style", "fill-color: rgb(200,0,0);");
			        }
			       	//Lets sleep 1 sec between renderings to show the user a nice and smooth graph building process
			        //sleep();		
	            }
	            
	        }
	        
	}
	catch (FileNotFoundException e) {e.printStackTrace();}
	updateMapToFile("isbnBooktitleMap"); //update our map to the file for future uses
	graph.display();   
	System.out.println("The red nodes are ISBN's and the black nodes are articles.");
	System.out.println("An edge between an ISBN and article means that the ISBN was referenced in that article.");
	System.out.println(graph.getNodeCount() + " nodes and " + graph.getEdgeCount() + " edges.");	
}


 protected static void sleep() {
        try { Thread.sleep(1000); } catch (Exception e) {}
    }

public static void readMapFromFile(String fileName) {
	//load map from file
	try {
		FileInputStream inStream = new FileInputStream(new File(fileName));
		ObjectInputStream outStream = new ObjectInputStream(inStream);
		isbn_to_title = (HashMap<String,String>) outStream.readObject(); //try to parse
		outStream.close();
		inStream.close();
		System.out.println("Succesfully read isbn map from file!");

	} catch (Exception e) {
		isbn_to_title = new HashMap<String,String>(); //if something went wrong, just create new
	}

}

public static void updateMapToFile(String fileName) {
	//update the map to the file for future usage
	try {
		FileOutputStream fOutStream = new FileOutputStream(new File(fileName));
		ObjectOutputStream oOutStream = new ObjectOutputStream(fOutStream);
		
		oOutStream.writeObject(isbn_to_title);

		oOutStream.close();
		fOutStream.close();

	} catch (Exception e) {
		System.out.println("Could not save title map");
	}

}

public static String getWikiName(String isbn){
	//return the corresponding book title for that isbn
	if(isbn_to_title.containsKey(isbn)) {
		return isbn_to_title.get(isbn); //isbn already exists in hashmap
	}
    String content = null; //save HTTP response here
    URLConnection connection = null;
    String title = ""; //save title here
    try {
		connection =  new URL("https://www.googleapis.com/books/v1/volumes?q=isbn:"+isbn).openConnection();
		Scanner scanner = new Scanner(connection.getInputStream());
		scanner.useDelimiter("\\Z");
		content = scanner.next();
		if(content.contains("\"title\"")) {
			// Line looks like this: "title": "Alan Turing Bibliography"
			int quoteCount= 0;
			for(int index = content.indexOf("\"title\""); index > 0; index++) {
				if(content.charAt(index) == '\"') {
					quoteCount++;
					if(quoteCount > 3) {
						break;
					}
				}
				if(quoteCount > 2 && content.charAt(index)!= '\"') {
					title = title + content.charAt(index);
					//System.out.println(result);
				}
			}
			
		}
		
		isbn_to_title.put(isbn, title); //add title to map
		scanner.close();
      
    }catch ( Exception ex ) {
      ex.printStackTrace();
    }
    
    return title;

  }
}

