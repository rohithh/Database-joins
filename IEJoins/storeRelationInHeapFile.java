package IEJoins;

import global.AttrType;
import global.RID;
import heap.Heapfile;
import heap.Tuple;

import java.util.List;

/**
 * Created by varun on 3/15/16.
 */
public class storeRelationInHeapFile {
    private boolean OK = true;
    private boolean FAIL = false;
    //List<RID> ridList;

    public void relationtoHeapFile(List<List<String>> relation, String name) throws Exception, Throwable{
         //This is for a single relation "S"
        long startTime = System.nanoTime();
        try {
            boolean status = OK;
            int rows = relation.size();
            int columns = relation.get(0).size();
            int NumberOfStrings = 0;
            AttrType[] Stypes = new AttrType[columns];
            //ridList = new ArrayList<RID>();
            //

            for (int i = 0; i < columns; i++) {
                if (relation.get(0).get(i).compareTo("attrInteger") == 0)
                    Stypes[i] = new AttrType(AttrType.attrInteger);
                else if (relation.get(0).get(i).compareTo("attrString") == 0) {
                    Stypes[i] = new AttrType(AttrType.attrString);
                    NumberOfStrings++;
                } else if (relation.get(0).get(i).compareTo("attrReal") == 0)
                    Stypes[i] = new AttrType(AttrType.attrReal);
                else {
                    FAIL = true;
                    break;
                }
            }
            short[] Ssizes;

            //-----------------------------------------------------------------------------
            if (NumberOfStrings > 0) {
                Ssizes = new short[NumberOfStrings];
                for (int i = 0; i < NumberOfStrings; i++)
                    Ssizes[i] = 30;         //first elt. is 30
            } else
                Ssizes = new short[1];

            //-----------------------------------------------------------------------------


            Tuple t = new Tuple();
            try {
                t.setHdr((short) columns, Stypes, Ssizes);
            } catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }
            //-----------------------------------------------------------------------------Only to get tuple size
            int size = t.size();

            RID rid;
            Heapfile f = null;

            try {
                name = name + ".in";
                f = new Heapfile(name);
            } catch (Exception e) {
                System.err.println("*** error in Heapfile constructor ***");
                status = FAIL;
                e.printStackTrace();
            }

            t = new Tuple(size);

            try {
                t.setHdr((short) columns, Stypes, Ssizes);
            } catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }
            //System.out.println(f.getRecCnt());
            if(f.getRecCnt()==0){

            for (int i = 1; i < rows; i++) {
                for (int j = 0; j < columns; j++) {
                    try {
                        if (Stypes[j].attrType == AttrType.attrInteger)
                            t.setIntFld(j + 1, (Integer.parseInt(relation.get(i).get(j))));
                        else if (Stypes[j].attrType == AttrType.attrString)
                            t.setStrFld(j + 1, (relation.get(i).get(j)));
                        else if (Stypes[j].attrType == AttrType.attrReal)
                            t.setFloFld(j + 1, (Float.parseFloat(relation.get(i).get(j))));
                    } catch (Exception e) {
                        System.err.println("*** Heapfile error in Tuple.setFld() ***");
                        status = FAIL;
                        e.printStackTrace();
                    }

                }
                try {
                    rid = f.insertRecord(t.returnTupleByteArray());
                    //ridList.add(rid);
                } catch (Exception e) {
                    System.err.println("*** error in Heapfile.insertRecord() ***");
                    status = FAIL;
                    e.printStackTrace();
                }

            }
            if (status != OK) {
                //bail out
                System.err.println("*** Error creating relation");
                Runtime.getRuntime().exit(1);
            }
        }
        }
        catch (Exception e){
            e.printStackTrace();
        }
        long endTime = System.nanoTime();
        System.out.println("Time to taken to store relation in heap file: (ms) "+(endTime-startTime)/1000000);
    }

    /*public void deleteHeapFiles(String name) throws Exception{
        Heapfile f = new Heapfile(name);
        f.deleteFile();
        System.out.println("Deleted");


        for(int i =0; i<ridList.size();i++){
            f.deleteRecord(ridList.get(i));
        }
        System.out.println("After "+ f.getRecCnt());

    }*/
}
