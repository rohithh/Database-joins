package IEJoins;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Created by varun on 3/13/16.
 */
public class queryUtils {

    public static final int quEQ   = 0;
    public static final int quLT   = 1;
    public static final int quGT   = 4;
    public static final int quGE   = 3;
    public static final int quLE   = 2;

    public final static int outer = 0;
    public final static int innerRel = 1;

    List<List<String>> listOListsQ = new ArrayList<List<String>>();


    public List<List<String>> readQueryFile(String nameOfTextFile){

        try {

            String finalFilePath = StartHere.filePath + "/" + nameOfTextFile;
            //String finalFilePath = StartHere.filePath + "/src/RelationNQuery/" + nameOfTextFile;

            List<String> singleList;

            File file = new File(finalFilePath);
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] ar = line.split(" ");
                singleList = Arrays.asList(ar);
                listOListsQ.add(singleList);

            }
            fileReader.close();
            //for(List i : listOListsQ)
            //    System.out.println(i);

        } catch (IOException e) {
            System.err.println("*** File not found ***");
            e.printStackTrace();
        }
        return listOListsQ;
    }

    public int getNumberOfRelations(){
        return listOListsQ.get(1).size();
    }

    public int getNumberOfPredicates(){
        if(listOListsQ.size()==3)
            return 1;
        else if(listOListsQ.size()==5)
            return 2;
        return 0;
    }

    public String getOuterRelation(){
        return listOListsQ.get(1).get(0);
    }

    public String getInnerRelation(){
        if(getNumberOfRelations()==2)
            return listOListsQ.get(1).get(1);
        else
            return listOListsQ.get(1).get(0);
    }

    public String printQuery(){
        String query = "";
                query = "SELECT "+ listToString(listOListsQ.get(0))
                + "\nFROM "+ listToString(listOListsQ.get(1))
                + "\nWHERE ";
        String extra = "";
        for(int i=2; i<listOListsQ.size();i++){
            extra = extra+ listToString(listOListsQ.get(i));
        }
        query = query+extra;
        return query;
    }

    public static String listToString(List<String> list){
        String listString = "";
        boolean flag = false;
        for (String s : list)
        {
            if(s.compareTo("1")==0 && !flag) {
                listString += "<" + " ";
                flag = true;
            }
            else if(s.compareTo("2")==0 && !flag) {
                listString += "<=" + " ";
                flag = true;
            }
            else if(s.compareTo("3")==0 && !flag) {
                listString += ">=" + " ";
                flag = true;
            }
            else if(s.compareTo("4")==0 && !flag) {
                listString += ">" + " ";
                flag = true;
            }
            else if(s.compareTo("0")==0 && !flag) {
                listString += "=" + " ";
                flag = true;
            }
            else
                listString += s + " ";
        }
        return listString;
    }
    public HashMap getWhereColumns(){
        HashMap hm = new HashMap();
        int something = 0;
        while (something<=getNumberOfRelations()) {
            for (int i = 2; i < listOListsQ.size(); i++) {
                for (int j = 0; j < listOListsQ.get(i).size(); j++) {
                    List list = new ArrayList();
                    if (listOListsQ.get(i).get(j).substring(0, 1).compareTo(listOListsQ.get(1).get(0)) == 0) {

                        list.add(listOListsQ.get(i).get(j).substring(2, 3));
                        something++;
                    } else if (listOListsQ.get(i).get(j).substring(0, 1).compareTo(listOListsQ.get(1).get(1)) == 0){
                        list.add(listOListsQ.get(i).get(j).substring(2, 3));
                        something++;
                    }
                    hm.put(listOListsQ.get(i).get(j).substring(0, 1),list);
                }
            }
        }
        return hm;
    }

}
