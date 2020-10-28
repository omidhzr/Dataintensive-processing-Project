import java.io.*;
import java.util.ArrayList;

import java.util.Scanner;
//import java.util.FileWriter;
//import java.util.BufferedWriter
//import java.util.PrintWriter


public class ProcessWikiDumps{
  //DUMP URL: https://dumps.wikimedia.org/enwiki/20201001/?fbclid=IwAR1LNT-2HrKJatj79nzczREO3Tb0gnqIgM3OsHShnkIRc2zpXyEq4-puF_A

  public static void main(String args[]){
    //https://en.wikipedia.org/wiki/Special:Random
    System.out.println("working...");
    preProcessFiles("wiki_dumps"); //process all files here
    //populateWikis(10); //Legacy - This method takes 10 random articles html... not so efficient...

  }

  public static void preProcessFiles(String directoryName){
    File directoryPath = new File(directoryName);
    File filesList[] = directoryPath.listFiles(); //get the files name in input
    if(filesList == null){
	System.out.println("Could not find the directory " + directoryName + " or it is empty");
	return;
    }
    ArrayList<String> fileNames = new ArrayList<String>(); //filenames
    ArrayList<String> processedFileNames = new ArrayList<String>();//store here
    for(File file : filesList){
      String name = file.getName();
      fileNames.add(name);
      System.out.println("Found " + name);
    }
    for(String name : fileNames){
      if(!name.contains("_processed") && !fileNames.contains(name+"_processed")){
        if(processFile(directoryName+"/"+name) > -1){
          processedFileNames.add(name+"_processed");
        }
      }
    }
  }

  public static int processFile(String filepath){
    FileInputStream inStream = null;
    Scanner sc = null;
    System.out.println(filepath);
    int pageCount = 0;
    try{
      inStream = new FileInputStream(filepath);
      sc = new Scanner(inStream);
      FileWriter writer = new FileWriter(filepath+"_processed",true);
      System.out.println("Processing "+filepath);
      BufferedWriter bwriter = new BufferedWriter(writer);
      PrintWriter out = new PrintWriter(bwriter);

      while(sc.hasNextLine()){
        String line = sc.nextLine();
        if(line.contains("<page>")){
          //each time we find a title-> "\ntitle"
          out.print("\n"+line);
          pageCount++;
          System.out.print("Processed " + pageCount + " pages.\r");
        }else{
          out.print(line);
        }
      }

      sc.close();
      writer.close();
    }catch(Exception e){
      System.out.println("Error processing file");
      return -1;//fail
    }

    System.out.println("Processed " + pageCount + " pages.");
    //We can delete the file now
    if ((new File(filepath)).delete()) {
      System.out.println("Deleted: " + filepath);
    } else {
      System.out.println("Failed to delete the unprocessed file...");
    }

    return pageCount;
  }
  public static void populateWikis(int count){
    String content = null;
    URLConnection connection = null;
    try {
      FileWriter writer = new FileWriter("input",true);
      BufferedWriter bwriter = new BufferedWriter(writer);
      PrintWriter out = new PrintWriter(bwriter); //we print with this
      for(int i = 0; i < count; i++){
        connection =  new URL("https://en.wikipedia.org/wiki/Special:Random").openConnection();
        Scanner scanner = new Scanner(connection.getInputStream());
        scanner.useDelimiter("\\Z");
        content = scanner.next();
        if(content.contains("<bdi>")){
          out.println(content.replace("\n","")); // write without \n
        }
        scanner.close();
      }

      writer.close();
    }catch ( Exception ex ) {
      ex.printStackTrace();
    }
  //  System.out.println(content);
  }
}
