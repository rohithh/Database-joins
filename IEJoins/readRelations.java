package IEJoins;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

/**
 * Created by varun on 3/12/16.
 */
public class readRelations {

    private Vector Q;
    private Vector R;
    private Vector S;
    private boolean FAIL = false;

    public static final int attrString  = 0;
    public static final int attrInteger = 1;
    public static final int attrReal    = 2;
    public static final int attrSymbol  = 3;


    public String relationName;
    List<String> GetAttributeTypes = new ArrayList<String>();
    public int numberOfRows = 0;


    /**
     *readFile method reads in the relation from a text file which has comma separated values,
     *the types of attributes have to be defined in the first line of the text file (it accepts integer, real, string)
     *@return List of List of lines in the text file
     *@param nameOfTextFile
     *@exception IOException  some I/O error
     */
    public List<List<String>> readRelationsFile(String nameOfTextFile) {

        List<List<String>> listOLists = new ArrayList<List<String>>();
        try {
            //System.out.println("Working Directory = " + System.getProperty("user.dir"));
            relationName = nameOfTextFile.substring(0,nameOfTextFile.indexOf('.'));
            String finalFilePath = StartHere.filePath + "/" + nameOfTextFile;
            //String finalFilePath = StartHere.filePath + "/src/RelationNQuery/" + nameOfTextFile;

            List<String> singleList;

            File file = new File(finalFilePath);
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] ar = line.split(",");
                singleList = Arrays.asList(ar);
                listOLists.add(singleList);
                numberOfRows++;
            }
            fileReader.close();


        } catch (IOException e) {
            e.printStackTrace();
        }
        GetAttributeTypes = listOLists.get(0);
        return listOLists;
    }


    public List<String> getAttributeTypes(){
        return GetAttributeTypes;
    }


    /*public List<String> getRow(int index){
        return listOLists.get(index);
    }*/

    /*public String getAttrType(int index){
        return listOLists.get(0).get(index);
    }*/

    public int getNumOfStringAtrr(){
        int num =0;
        for(int i = 0; i<GetAttributeTypes.size();i++){
            if(GetAttributeTypes.get(i).compareTo("attrString")==0)
                num++;
        }
        if(num>0)
            return num;
        else
            return 1;
    }

}
