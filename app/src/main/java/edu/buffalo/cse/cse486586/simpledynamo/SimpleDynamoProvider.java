package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.text.LoginFilter;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    static final String TAG = "TAG";
    boolean isRecovered = false;
    Sqlhelper mydb;
    SQLiteDatabase database;
    String portStr, myPort;
    String[] cols = new String[]{"key", "value"};
    MatrixCursor matrixCursor = new MatrixCursor(cols);
    MatrixCursor tCursor= new MatrixCursor(cols);
    HashMap<String, ArrayList<String>> waitInsertMap = new HashMap<String, ArrayList<String>>();
    HashMap<String, Boolean> waitQueryMap = new HashMap<String, Boolean>();
    HashMap<String,String>cursorMap= new HashMap<>();
    int coordinator, successor1, successor2;

    int querycount = 0;
    HashMap<String, String> portKeyMap = new HashMap<>();
    ArrayList<String> nodePeerList = new ArrayList<String>(5);

    public static final Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        if (selection.equals("\"@\"") ) {

            return database.delete(Sqlhelper.TABLE_NAME, "1", null);
        }
        else if(selection.equals("\"*\"")){
            deleteStar(selection);
        }
        else{


            try {

                Log.d(TAG, "Inside insert:");
                String keyhash = genHash(selection);
                Log.d(TAG, "Inside insert: key hash" + keyhash);
                Log.d(TAG, "Inside insert: Entering For loop");

                for (int i = 0; i < 5; i++) {

                    Log.d(TAG, "Inside delete specific:  Inside For loop :: i =" + i);
                    if ((keyhash.compareTo(nodePeerList.get(i)) < 0)) {

                        Log.d(TAG, "Inside delete specific:  Inside FOR loop :: inside IF (keyhash.compareTo(nodePeerList.get(i))");

                        makeDeletePacketToSend(i, selection);
                        break;
                    } else if ((keyhash.compareTo(nodePeerList.get(i)) > 0) && (keyhash.compareTo(nodePeerList.get(4))) > 0) {

                        Log.d(TAG, "Inside delete specific:  Inside FOR loop : ::inside IF ((keyhash.compareTo(nodePeerList.get(i))>0)&&(keyhash.compareTo(nodePeerList.get(4)))");
                        makeDeletePacketToSend(i,selection);

                        break;
                    }
                }
                return 1;
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            return 1;


        }


        Log.v("delete", selection);
        return 1;
    }

    private void makeDeletePacketToSend(int i, String selection) {

        ArrayList<String> toSend = new ArrayList<>();

        if (i < 3) {
            coordinator = i;
            successor1 = i + 1;
            successor2 = i + 2;
        } else if (i == 3) {
            coordinator = 3;
            successor1 = 4;
            successor2 = 0;
        } else if (i == 4) {
            coordinator = 4;
            successor1 = 0;
            successor2 = 1;
        }

        toSend.add("Delete_Key");
        toSend.add(portKeyMap.get(nodePeerList.get(coordinator)));
        toSend.add(portKeyMap.get(nodePeerList.get(successor1)));
        toSend.add(portKeyMap.get(nodePeerList.get(successor2)));
        toSend.add(selection);
        toSend.add("Garbage");
        toSend.add(portStr);

        Log.d(TAG, "Created message in delete() message: " + toSend.toString());

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toSend);

    }


    private void deleteStar(String selection){


        for (int i = 0; i < 5; i++) {

            Log.d(TAG, "Inside query: Star:  Inside For loop :: i =" + i);

            ArrayList<String> toSend = new ArrayList<String>();
            toSend.add("Delete_Star");
            toSend.add(portKeyMap.get(nodePeerList.get(i)));
            toSend.add(selection);
            toSend.add(portStr);

            Log.d(TAG, "Inside query: Star: Entering ClientTask tosend= " + toSend.toString());
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toSend);

        }



    }

    private class DeleteTask extends AsyncTask<ArrayList<String>, Void, Void> {

        @Override
        protected Void doInBackground(ArrayList<String>... msgs) {

            return null;
        }
    }
    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        try {


            Log.d(TAG, "Inside insert:");
            String keyhash = genHash((String) values.get("key"));
            Log.d(TAG, "Inside insert: key hash" + keyhash);
            Log.d(TAG, "Inside insert: Entering For loop");

            for (int i = 0; i < 5; i++) {

                Log.d(TAG, "Inside insert:  Inside For loop :: i =" + i);
                if ((keyhash.compareTo(nodePeerList.get(i)) < 0)) {

                    Log.d(TAG, "Inside insert:  Inside FOR loop :: inside IF (keyhash.compareTo(nodePeerList.get(i))");
                    ArrayList<String> toSend = new ArrayList<String>();
                    toSend = makeInsertPacketToSend(i, values);
                    insertPacketAndWait(toSend);
                    break;
                } else if ((keyhash.compareTo(nodePeerList.get(i)) > 0) && (keyhash.compareTo(nodePeerList.get(4))) > 0) {

                    Log.d(TAG, "Inside insert:  Inside FOR loop : ::inside IF ((keyhash.compareTo(nodePeerList.get(i))>0)&&(keyhash.compareTo(nodePeerList.get(4)))");
                    ArrayList<String> toSend = new ArrayList<String>();
                    toSend = makeInsertPacketCornerCase(values);
                    insertPacketAndWait(toSend);
                    break;
                }
            }
            return uri;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return uri;
    }

    private ArrayList makeInsertPacketToSend(int i, ContentValues values) {

        ArrayList<String> toSend = new ArrayList<>();

        if (i < 3) {
            coordinator = i;
            successor1 = i + 1;
            successor2 = i + 2;
        } else if (i == 3) {
            coordinator = 3;
            successor1 = 4;
            successor2 = 0;
        } else if (i == 4) {
            coordinator = 4;
            successor1 = 0;
            successor2 = 1;
        }

        toSend.add("Insert_First");
        toSend.add(portKeyMap.get(nodePeerList.get(coordinator)));
        toSend.add(portKeyMap.get(nodePeerList.get(successor1)));
        toSend.add(portKeyMap.get(nodePeerList.get(successor2)));
        toSend.add((String) values.get("key"));
        toSend.add((String) values.get("value"));
        toSend.add(portStr);

        Log.d(TAG, "Created message in insert() message: " + toSend.toString());
        return toSend;

    }

    private ArrayList makeInsertPacketCornerCase(ContentValues values) {
        ArrayList<String> toSend = new ArrayList<>();

        coordinator = 0;
        successor1 = 1;
        successor2 = 2;

        toSend.add("Insert_First");
        toSend.add(portKeyMap.get(nodePeerList.get(coordinator)));
        toSend.add(portKeyMap.get(nodePeerList.get(successor1)));
        toSend.add(portKeyMap.get(nodePeerList.get(successor2)));
        toSend.add((String) values.get("key"));
        toSend.add((String) values.get("value"));
        toSend.add(portStr);

        Log.d(TAG, "Created message in insert() message: " + toSend.toString());
        return toSend;
    }

    private void insertPacketAndWait(ArrayList toSend) {

        String sendKey = toSend.get(4) + "#" + portStr;
        waitInsertMap.put(sendKey, new ArrayList<String>());
        Log.d(TAG, "Inside insert: waitInsertMap : " + waitInsertMap.toString());

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toSend);

        Log.d(TAG, "Inside insert: waitInsertMap : " + waitInsertMap.toString());

        while (waitInsertMap.get(toSend.get(4) + "#" + portStr).size() < 2) {


        }
       // waitInsertMap.put(sendKey, new ArrayList<String>());
        Log.d(TAG, "Inside insert:  Entering ClientTask tosend= " + toSend.toString());


    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub





        Log.d(TAG, "Inside onCreate");
        mydb = new Sqlhelper(getContext());
        database = mydb.getWritableDatabase();

        Log.d(TAG, "Inside onCreate: database setup");
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        Log.d(TAG, "Inside onCreate: telephony- portStr: " + portStr + " and myPort: " + myPort);

        setLocalInfoList();

        ArrayList<String> toSend= new ArrayList<>();
        toSend.add("NodeRecovery");
        new RecoverTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,toSend);
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            ServerSocket serverSocket = new ServerSocket(10000);
            Log.d(TAG, "Oncreate: creating Servertask");
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "oncreate: Can't create a ServerSocket");
        }
        



        if (database != null)
            return true;
        else
            return false;

    }

    private void recoverAllData(String[] successors,String [] predecessors){

        int mode = 0;
        while (mode < 2) {

            Log.d(TAG,"mode :" + mode);
            ArrayList<String> toSend = new ArrayList<>();

            if (mode == 0) {

                Log.d(TAG,"recoverAllData: Recovery : Successor Mode");
                toSend.add("RecoverMode");
                toSend.add("SuccessorMode");
                toSend.add(portStr);


            } else if (mode == 1) {

                Log.d(TAG,"recoverAllData: Recovery : Predecessor Mode");
                toSend.add("RecoverMode");
                toSend.add("PredecessorMode");
                toSend.add(portStr);

            }


            for (int i = 0; i <= 1; i++) {

                Log.d(TAG,"Oncreate: recoverAll data : For Loop-> i = "+i);
                Socket socket = null;
                try {

                    if (mode==0) {

                        Log.d(TAG,"Oncreate : recoverAllData : Sending in mode = 0 to  "+successors[i]);
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(successors[i]) * 2);

                    } else if(mode == 1){
                        Log.d(TAG,"Oncreate : recoverAllData : Sending in mode = 1 to  "+predecessors[i]);
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(predecessors[i]) * 2);
                    }
                    Log.d(TAG,"RecoverAllData : Sent toSend :" + toSend.toString());

                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(toSend);
                    out.flush();

                    Log.d(TAG,"out flushed");

                    Log.d(TAG, "ClientTask: out sent..");

                    ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
                    Log.d(TAG, "ClientTask: input");
                    List<String> messageList1 = new ArrayList<>();
                    messageList1 = (List<String>) input.readObject();

                    Log.d(TAG,"RecoverAllData : Received messageList :" +messageList1.toString());
                    for (int j = 0; j < messageList1.size(); j++) {
                        Log.d(TAG,"RecoverAllData : Entered for 2nd Loop with j = "+j);

                        String pair = messageList1.get(j);
                        String pairs[] = pair.split(";");

                        ContentValues cvalues = new ContentValues();
                        cvalues.put("key", pairs[0]);
                        cvalues.put("value", pairs[1]);


                        database.insert(Sqlhelper.TABLE_NAME, null, cvalues);
                        Log.d(TAG,"RecoverAllData : Inserted into database : "+ cvalues.toString());
                    }
                    input.close();
                    out.close();
                    socket.close();


                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }

            }

            mode++;
        }
        isRecovered = true;

    }

    private class RecoverTask extends AsyncTask<ArrayList<String>, Void, Void> {

        @Override
        protected Void doInBackground(ArrayList<String>... msgs) {

            isRecovered = false;
            Log.d(TAG,"Entered RecoveryTask");

            String[] successors = new String[2];
            String[] predecessors = new String[2];

            //finding the successors and predecessors

            successors = findSuccessors(portStr);
            Log.d(TAG,"RecoveryTask : received successors 1:"+ successors[0].toString()+" 2: "+successors[1].toString());
            predecessors = findPredecessors(portStr);
            Log.d(TAG,"RecoveryTask : received predecessors :"+ predecessors[0].toString()+"2 : "+predecessors[1].toString());
            recoverAllData(successors, predecessors);
            Log.d(TAG,"RecoveryTask : Recovered all data :");

            return null;
        }
    }


    private void setLocalInfoList() {

        try {
            portKeyMap.put(genHash("5554"), "5554");
            nodePeerList.add(genHash("5554"));
            Collections.sort(nodePeerList);

            portKeyMap.put(genHash("5556"), "5556");
            nodePeerList.add(genHash("5556"));
            Collections.sort(nodePeerList);

            portKeyMap.put(genHash("5558"), "5558");
            nodePeerList.add(genHash("5558"));
            Collections.sort(nodePeerList);

            portKeyMap.put(genHash("5560"), "5560");
            nodePeerList.add(genHash("5560"));
            Collections.sort(nodePeerList);

            portKeyMap.put(genHash("5562"), "5562");
            nodePeerList.add(genHash("5562"));
            Collections.sort(nodePeerList);

            for (int j = 0; j < 5; j++) {
                Log.d(TAG, "Oncreate: nodePeerlist" + nodePeerList.get(j));
            }

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub

        Log.d(TAG, "Inside Query");
        Log.d(TAG, "Key: " + selection + "sortOrder: " + sortOrder);
        String args[] = {selection};

        if (selection.contains("@")) {

            matrixCursor = new MatrixCursor(cols);

            return database.rawQuery("Select * from " + Sqlhelper.TABLE_NAME, null);
        } else if (selection.contains("*")) {

            Log.d(TAG, "Inside QueryStar : " + selection);

                if (sortOrder == null) {
                    matrixCursor = new MatrixCursor(cols);
                    Log.d(TAG, "Inside query : star: Entering For loop");

                    sendQueryStarToAll(selection);
                    waitUntilQueryStarReturn();
                    printMatrixCursor(matrixCursor);
                    return matrixCursor;

                } else {

                    Log.d(TAG, "Inside query: query request received ");
                    bundleQueryStarAndSend(sortOrder);
                    return null;

                }

        } else {
            //Specific query block


            Log.d(TAG, "Inside Query specific key: " + selection);
            String keyhash = null;
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if(isRecovered == false){

            }

            try {

                keyhash = genHash(selection);
                String queryDestination;

                Log.d(TAG,"Inside Query_Specific:");
                MatrixCursor tCursor=new MatrixCursor(cols);
                MatrixCursor matrixCursor = new MatrixCursor(cols);
                Log.d(TAG, "Inside query specific: Entering For loop");

                for (int i = 0; i < 5; i++) {

                    Log.d(TAG, "Inside query specific:  Inside For loop :: i =" + i);

                    if ((keyhash.compareTo(nodePeerList.get(i)) < 0)) {

                        Log.d(TAG, "Inside query specific:  Inside For loop :if((keyhash.compareTo(nodePeerList.get(i))<0)) : i =" + i);
                        ArrayList<String> tosend = new ArrayList<String>();
                        String qhashkey = nodePeerList.get(i);
                        Log.d(TAG, "nodePeerlist : " + nodePeerList.toString() + " current entry : " + qhashkey);
                        queryDestination = portKeyMap.get(qhashkey);
                        Log.d(TAG, "Query specific: portkeymap: " + portKeyMap.toString() + "QueryDestination: " + queryDestination);
                        tCursor=bundleQuerySpecificThenSendAndWait(selection, queryDestination);
                        break;


                    } else if ((keyhash.compareTo(nodePeerList.get(i)) > 0) && (keyhash.compareTo(nodePeerList.get(4))) > 0) {

                        ArrayList<String> toSend = new ArrayList<String>();
                        Log.d(TAG, "Inside query specific: else if((keyhash.compareTo(nodePeerList.get(i))>0)&&(keyhash.compareTo(nodePeerList.get(4)))>0): i =" + i);
                        String qhashkey = nodePeerList.get(0);
                        Log.d(TAG, "nodePeerlist : " + nodePeerList.toString() + " current entry : " + qhashkey);
                        queryDestination = portKeyMap.get(qhashkey);
                        Log.d(TAG, "Query specific: portkeymap: " + portKeyMap.toString() + "QueryDestination: " + queryDestination);
                        tCursor=bundleQuerySpecificThenSendAndWait(selection, queryDestination);


                        break;

                    }
                }

                Log.d(TAG, "Inside query specific: returning tCursor : ");

                printMatrixCursor(tCursor);
                return tCursor;


            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

        }

        Cursor cursor = database.query(Sqlhelper.TABLE_NAME, projection, Sqlhelper.Keyval + "=?", args, null, null, null);

        return cursor;
    }

    private ArrayList QueryMyDatabaseAndSendPacket(Uri uri, String[] projection, String selection, String origin) {

        String[] args = {selection};
        Log.d(TAG, "Inside query specific: Else: query request received ");

        Cursor cursor = database.query(Sqlhelper.TABLE_NAME, projection, Sqlhelper.Keyval + "=?", args, null, null, null);
        ArrayList<String> toSend = new ArrayList<>();

        toSend.add("Query_Complete");
        toSend.add(origin);

        cursor.moveToFirst();
        String sendKey = cursor.getString(cursor.getColumnIndex("key"));
        String sendVal = cursor.getString(cursor.getColumnIndex("value"));
        toSend.add(sendKey);
        toSend.add(sendVal);
        toSend.add(portStr);


        Log.d(TAG, "Leaving query:  Entering ClientTask tosend= " + toSend.toString());

        return toSend;



    }

    private MatrixCursor bundleQuerySpecificThenSendAndWait(String selection, String queryDestination) {

        ArrayList<String> toSend = new ArrayList<>();
        MatrixCursor tempCursor = new MatrixCursor(cols);

        toSend.add("Query_Key");
        toSend.add(queryDestination);
        toSend.add(selection);
        toSend.add(portStr);

        String sendQkey = toSend.get(2) + "#" + queryDestination;
        waitQueryMap.put(sendQkey, false);

        Log.d(TAG, "Inside query specific: waitQueryMap : " + waitQueryMap.toString());
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toSend);

        Log.d(TAG, "Inside query specific: toSend : " + toSend.toString());
        Log.d(TAG, "Inside query specific: waitQueryMap : " + waitQueryMap.toString());


        while (!waitQueryMap.get(sendQkey)) {
            Log.d(TAG, "Inside query specific: waiting for value : " + waitQueryMap.get(sendQkey));
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
        String cursorKey = selection;
        String cursorValue = cursorMap.get(selection);
        tempCursor.addRow(new Object[]{cursorKey,cursorValue});

        Log.d(TAG, "Inside query specific: waitQueryMap : " + waitQueryMap.toString());
        Log.d(TAG, "Breaking now");

        return tempCursor;
    }

    private ArrayList bundleQueryStarAndSend(String sortOrder) {

        ArrayList<String> toSend = new ArrayList<>();
        toSend.add("Query_StarComplete");
        toSend.add(sortOrder);

        Cursor cursor = database.rawQuery("Select * from " + Sqlhelper.TABLE_NAME, null);
        cursor.moveToFirst();

        while (cursor.isAfterLast() == false)
        {

            String sendKey = cursor.getString(cursor.getColumnIndex("key"));
            String sendVal = cursor.getString(cursor.getColumnIndex("value"));
            toSend.add(sendKey + "," + sendVal);
            cursor.moveToNext();

        }

        Log.d(TAG, "Leaving query:  Entering ClientTask tosend= " + toSend.toString());
        return toSend;
    }

    private void sendQueryStarToAll(String selection) {

        for (int i = 0; i < 5; i++) {

            Log.d(TAG, "Inside query: Star:  Inside For loop :: i =" + i);

            ArrayList<String> toSend = new ArrayList<String>();
            toSend.add("Query_Star");
            toSend.add(portKeyMap.get(nodePeerList.get(i)));
            toSend.add(selection);
            toSend.add(portStr);

            Log.d(TAG, "Inside query: Star: Entering ClientTask tosend= " + toSend.toString());
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toSend);

        }

    }

    private void waitUntilQueryStarReturn() {

        while (querycount < 4) {

        }
        querycount = 0;

    }


    private class ClientTask extends AsyncTask<ArrayList<String>, Void, Void> {

        @Override
        protected Void doInBackground(ArrayList<String>... msgs) {
            try {
                Log.d(TAG, "Inside ClientTask: doInBackground");
                String msgToSend = msgs[0].get(0);
                Socket socket;

                Log.d(TAG, "Inside ClientTask: doInBackground msgtoSend: " + msgToSend.toString());
                if (msgToSend.equals("Insert_First")) {

                    Log.d(TAG, "ClientTask: Insert_Key: Inside MarketMessage = " + msgToSend.toString());
                    Log.d(TAG, "Inside ClientTask: doInBackground::Entering of for loop");

                    for ( int k = 1; k <= 3; k++){
                        if (msgs[0].get(k).equals(portStr)){

                            Log.d(TAG,"Same node insert");
                            ContentValues cvalues = new ContentValues();
                            cvalues.put("key", msgs[0].get(4));
                            cvalues.put("value", msgs[0].get(5));

                            database.insert(Sqlhelper.TABLE_NAME, null, cvalues);
                            Log.d(TAG, "ServerTask: inserted in database Cvalues= " + cvalues);

                            String iKey = msgs[0].get(4) + "#" + portStr;
                            String destinationPort = portStr;
                            waitInsertMap.get(iKey).add(destinationPort);
                            Log.d(TAG, "ClientTask: Insert_Key: inserttoNodes: inserted into wait waitInsertMap= " + waitInsertMap.toString());

                        }
                        else{

                            insertToNodes(msgs[0],k);

                        }


                    }

                    Log.d(TAG, "Inside ClientTask: doInBackground:: out of for loop");
                    Log.d(TAG, "ClientTask: Insert_Complete done" + msgs[0].toString());
                    Log.d(TAG, "ClientTask: Insert_Complete done" + msgs[0].toString());
                }
                else if (msgToSend.equals("Query_Key")) {

                    Log.d(TAG, "ClientTask: Query_Key: Inside MarketMessage = " + msgToSend.toString());
                    Log.d(TAG, "ClientTask: Query_Key:+msgPacket :" + msgs[0].toString() + " Destination port: " + msgs[0].get(1));

                    String destinationPort =msgs[0].get(1);
                    int isCoordinatorPresent=queryToNode(msgs[0]);
                    Log.d(TAG,"Coordinator value :"+isCoordinatorPresent);



                    if(isCoordinatorPresent == 0){

                        queryRecoveryFromSuccessor(msgs[0],destinationPort);
                    }

                }
                else if (msgToSend.equals("Query_Star")) {


                    Log.d(TAG, "ClientTask: Query_Star: msgtosend = " + msgToSend.toString() + " portStr : " + portStr + "destination port = " + msgs[0].toString());

                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[0].get(1)) * 2);
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(msgs[0]);
                    out.flush();


                    Log.d(TAG,"ClientTask: out sent..");
                    ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
                    Log.d(TAG, "ClientTask: input");



                    ArrayList<String> messageList = (ArrayList<String>) input.readObject();

                    Log.d(TAG, "ServerTask: Query_StarComplete: messagelist : " + messageList.toString());

                    for (int i = 2; i < messageList.size(); i++) {
                        String pair = messageList.get(i);
                        String pairs[] = pair.split(",");
                        matrixCursor.addRow(pairs);


                    }
                    Log.d(TAG, "matrixcursor count =" + matrixCursor.getCount());

                    Log.d(TAG, "Leaving ServerTask: Query_StarComplete");
                    querycount++;
                    Log.d(TAG, "querycount" + querycount);

                    Log.d(TAG, "ClientTask: Query_Star done");
                }
                else if (msgToSend.equals("Query_StarComplete")) {

                    Log.d(TAG, "ClientTask: Query_Complete: msgtosend = " + msgToSend.toString() + " portStr : " + portStr);
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[0].get(1)) * 2);
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(msgs[0]);
                    out.close();

                    Log.d(TAG, "ClientTask: Query_StarComplete done");
                }
                else if(msgToSend.equals("Delete_Star")){

                    Log.d(TAG, "ClientTask: Delete_Star: msgtosend = " + msgToSend.toString() + " portStr : " + portStr + "destination port = " + msgs[0].toString());

                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[0].get(1)) * 2);
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(msgs[0]);
                    out.flush();

                    Log.d(TAG,"ClientTask: Delete_Star :out sent..");
                    Log.d(TAG, "ClientTask: Delete_Star done");


                }
                else if(msgToSend.equals("Delete_Key")){

                    Log.d(TAG, "ClientTask: Insert_Key: Inside MarketMessage = " + msgToSend.toString());
                    Log.d(TAG, "Inside ClientTask: doInBackground::Entering of for loop");

                    for ( int k = 1; k <= 3; k++){

                        deleteToNodes(msgs[0],k);

                    }

                    Log.d(TAG, "Inside ClientTask: doInBackground:: out of for loop");
                    Log.d(TAG, "ClientTask: Insert_Complete done" + msgs[0].toString());
                    Log.d(TAG, "ClientTask: Insert_Complete done" + msgs[0].toString());

                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            return null;
        }
    }

    private void queryRecoveryFromSuccessor(ArrayList<String> msgs, String destinationPort){



        ArrayList<String> msgFailedQuery = new ArrayList<>();
        msgFailedQuery= msgs;
        Socket socket;

        Log.d(TAG,"ClientTask: queryRecoveryFromSuccessor:Entered Failed Query Specific Mode");
        String[] successor = findSuccessors(destinationPort);

        String originOfFailedQuery = msgFailedQuery.get(1);
        ArrayList<String> toSend = new ArrayList<>();
        toSend.add("QueryKeyFailedMode");
        toSend.add(successor[0]); //successor
        toSend.add(msgFailedQuery.get(2));//key
        toSend.add(destinationPort);//origin

        Log.d(TAG,"toSend :"+toSend);
        Log.d(TAG,"ClientTask: queryRecoveryFromSuccessor: Sending to Destination :"+destinationPort);




        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(toSend.get(1)) * 2);

            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(toSend);
            out.flush();


            Log.d(TAG,"ClientTask: queryRecoveryFromSuccessor:Out Sent ");
            ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
            Log.d(TAG, "ClientTask: queryRecoveryFromSuccessor: ClientTask: input");
            ArrayList<String> messageList = (ArrayList<String>) input.readObject();

            Log.d(TAG, "ClientTask: queryRecoveryFromSuccessor:ServerTask: Query_Complete: messagelist : " + messageList.toString());
            String keyQuery = messageList.get(2);
            String keyValue = messageList.get(3);
            Log.d(TAG, "ClientTask:  Query_Complete: calling query() with key: " + keyQuery + "value: " + keyValue);
            String senderPort = messageList.get(4); //sender's port
            String coordinator = msgFailedQuery.get(1);

            String Qkey = keyQuery + "#" + coordinator;


            cursorMap.put(keyQuery,keyValue);
            waitQueryMap.put(Qkey, true);
            Log.d(TAG,"ClientTask: queryRecoveryFromSuccessor: cursorMap :"+ cursorMap.toString());


            //tCursor.addRow(new Object[]{keyQuery, keyValue});
            Log.d(TAG, "ClientTask: queryRecoveryFromSuccessor: Query Complete: Qkey: " + Qkey);
            Log.d(TAG, "ClientTask: queryRecoveryFromSuccessor:ServerTask Query complete: matrixCursor on port : " + portStr);

            //printMatrixCursor(tCursor);
            Log.d(TAG, "ClientTask: queryRecoveryFromSuccessor: Servertask: Updatd waitQueryMap :" + waitQueryMap.toString());
            Log.d(TAG, "ClientTask: queryRecoveryFromSuccessor: Leaving ServerTask: Query_Complete " + messageList.toString());

            Log.d(TAG, "ClientTask: queryRecoveryFromSuccessor: Query_Key done msgPacket: " + msgs.toString());
            out.close();
            input.close();
            socket.close();


        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


    }

    private void insertToNodes(ArrayList<String> msgs, int k){


        Log.d(TAG,"Inside  Insert_Key: insertToNodes with msgs : "+msgs+" int k = "+k);
        ArrayList<String>msgInsert = new ArrayList<>();
        int m = k;

        msgInsert= msgs;
        Log.d(TAG,"Inside Insert_Key: insertToNodes with msgInsert : "+msgInsert);
        Socket socket = null;
        try {

            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(msgInsert.get(m)) * 2);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

            Log.d(TAG,"insertToNodes :Insert_Key: out object created");
            out.writeObject(msgInsert);

            out.flush();

            Log.d(TAG,"inserttonodes : Insert_Key: Out done..Testing input now");

            ObjectInputStream input = new ObjectInputStream(socket.getInputStream());

            Log.d(TAG, "ClientTask: Insert_Key: inserttoNodes : input");
            ArrayList<String> messageList = (ArrayList<String>) input.readObject();
            Log.d(TAG, "ClientTask: Insert_Key: inserttonodes : Insert_Key:Received input messagelist: " + messageList.toString());

            Log.d(TAG, "ClientTask: Insert_Key:IinserttoNodes: nside Insert_Complete with messagelist : " + messageList.toString());
            String iKey = messageList.get(2) + "#" + messageList.get(1);
            String destinationPort = messageList.get(3);
            waitInsertMap.get(iKey).add(destinationPort);
            Log.d(TAG, "ClientTask: Insert_Key: inserttoNodes: inserted into wait waitInsertMap= " + waitInsertMap.toString());
            Log.d(TAG, "ClientTask: Insert_Key: inserttoNodes: Leaving Insert_Complete with messagelist : " + messageList.toString());
            input.close();
            out.close();
            socket.close();

        }
        catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


    }
    private void deleteToNodes(ArrayList<String> msgs, int k){


        Log.d(TAG,"Inside  Insert_Key: deleteToNodes with msgs : "+msgs+" int k = "+k);
        ArrayList<String>msgInsert = new ArrayList<>();
        int m = k;

        msgInsert= msgs;
        Log.d(TAG,"Inside Insert_Key: deleteToNodes with msgInsert : "+msgInsert);
        Socket socket = null;
        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(msgInsert.get(m)) * 2);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

            Log.d(TAG,"deleteToNodes :Insert_Key: out object created");
            out.writeObject(msgInsert);

            out.flush();

            out.close();
            socket.close();

        }
        catch (IOException e) {
            e.printStackTrace();
        }


    }


        private int queryToNode(ArrayList<String> msgs){

        Socket socket;
        ArrayList<String>msgQuery = new ArrayList<>();
        msgQuery= msgs;

        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(msgQuery.get(1)) * 2);


            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(msgQuery);
            out.flush();

            Log.d(TAG,"ClientTask: out sent..");
            ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
            Log.d(TAG, "ClientTask: input");
            ArrayList<String> messageList = (ArrayList<String>) input.readObject();

            Log.d(TAG, "ServerTask: Query_Complete: messagelist : " + messageList.toString());
            String keyQuery = messageList.get(2);
            String keyValue = messageList.get(3);
            Log.d(TAG, "ServerTask: Query_Complete: calling query() with key: " + keyQuery + "value: " + keyValue);
            String senderPort = messageList.get(4); //sender's port
            String coordinator = msgQuery.get(1);

            String Qkey = keyQuery + "#" + coordinator;


            waitQueryMap.put(Qkey, true);
            cursorMap.put(keyQuery,keyValue);
            Log.d(TAG," cursorMap :"+ cursorMap.toString());


            //tCursor.addRow(new Object[]{keyQuery, keyValue});
            Log.d(TAG, "ServerTask Query Complete: Qkey: " + Qkey);
            Log.d(TAG, "ServerTask Query complete: matrixCursor on port : " + portStr);

            //printMatrixCursor(tCursor);
            Log.d(TAG, "Servertask: Updatd waitQueryMap :" + waitQueryMap.toString());
            Log.d(TAG, "Leaving ServerTask: Query_Complete " + messageList.toString());

            //out.close();
            //socket.close();

            Log.d(TAG, "ClientTask: Query_Key done msgPacket: " + msgQuery.toString());

        } catch (IOException e) {
            return 0;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return 1;
    }


    private String[] findSuccessors(String port){

        String[] successor = new String[2];
        

        if(port.equals("5562")){
            successor[0] = "5556";
            successor[1] = "5554";
        }
        else if(port.equals("5556")){
            successor[0] = "5554";
            successor[1] = "5558";
        }
        else if(port.equals("5554")){
            successor[0] = "5558";
            successor[1] = "5560";
        }
        else if(port.equals("5558")){
            successor[0] = "5560";
            successor[1]= "5562";
        }
        else if(port.equals("5560")){
            successor[0] = "5562";
            successor[1] = "5556";
        }

        return successor;

    }

    private String[] findPredecessors(String port){

        String[] predecessor = new String[2];


        if(port.equals("5562")){
            predecessor[0] = "5560";
            predecessor[1] = "5558";
        }
        else if(port.equals("5556")){
            predecessor[0] = "5562";
            predecessor[1] = "5560";
        }
        else if(port.equals("5554")){
            predecessor[0] = "5556";
            predecessor[1] = "5562";
        }
        else if(port.equals("5558")){
            predecessor[0] = "5554";
            predecessor[1]= "5556";
        }
        else if(port.equals("5560")){
            predecessor[0] = "5558";
            predecessor[1] = "5554";
        }

        return predecessor;

    }

    private List getRecoveredEntries(ArrayList messageList){

        List<String> toSend = new ArrayList<>();
        List<String> message = new ArrayList<>();

        Log.d(TAG,"Entered getRecoveredEntries");
        String myHash = null;
        message = messageList;
        String[] predecessors = null ;
        try {

            Log.d(TAG,"MessageList :"+message.toString());


            if (message.get(1).equals("SuccessorMode")) {
                Log.d(TAG,"ServerTask :getRecoveredEntries : Successor Mode");
                myHash = genHash(message.get(2));
                predecessors = findPredecessors(message.get(2));
                Log.d(TAG,"hash :"+myHash);
            } else if (message.get(1).equals("PredecessorMode")) {
                Log.d(TAG,"ServerTask :getRecoveredEntries : Predecessor Mode");
                myHash = genHash(portStr);
                predecessors = findPredecessors(portStr);
                Log.d(TAG,"hash :"+myHash);
            }

            Cursor cursor = database.rawQuery("Select * from " + Sqlhelper.TABLE_NAME, null);
            Log.d(TAG,"ServerTask : getRecoveredEntries : Queried * into database");
            cursor.moveToFirst();

            while (cursor.isAfterLast() == false)
            {
                Log.d(TAG," getRecoveredEntries : cursor count : "+cursor.getCount());
                String sendKey = cursor.getString(cursor.getColumnIndex("key"));
                String sendVal = cursor.getString(cursor.getColumnIndex("value"));


                String keyHash = genHash((sendKey));
                String predHash = genHash(predecessors[0]);
                Log.d(TAG,"predecessorHash :"+predHash);
                Log.d(TAG,"myHash :"+myHash);
                Log.d(TAG,"KeyHash :"+keyHash);

                if(keyHash.compareTo(predHash)>0 &&(keyHash.compareTo(myHash)<=0)){

                    Log.d(TAG,"ServerTask :getRecoveredEntries : Found Belonging key :"+sendKey+" value :"+sendVal);
                    toSend.add(sendKey + ";" + sendVal);
                    Log.d(TAG,"ServerTask :getRecoveredEntries : Added to toSend : " +toSend);
                    Log.d(TAG," toSend Count :"+toSend.size());


                }   else if (keyHash.compareTo(predHash) <= 0 && keyHash.compareTo(myHash) <= 0 && predHash.compareTo(myHash) > 0){

                    Log.d(TAG,"ServerTask :getRecoveredEntries : Found Belonging key :"+sendKey+" value :"+sendVal);
                    toSend.add(sendKey + ";" + sendVal);
                    Log.d(TAG,"ServerTask :getRecoveredEntries : Added to toSend : " +toSend);
                    Log.d(TAG," toSend Count :"+toSend.size());

                }else if (keyHash.compareTo(predHash) > 0 && predHash.compareTo(myHash) > 0){

                    Log.d(TAG,"ServerTask :getRecoveredEntries : Found Belonging key :"+sendKey+" value :"+sendVal);
                    toSend.add(sendKey + ";" + sendVal);
                    Log.d(TAG,"ServerTask :getRecoveredEntries : Added to toSend : " +toSend);
                    Log.d(TAG," toSend Count :"+toSend.size());

                }


                Log.d(TAG," keyHash  :"+keyHash+ " sendKey :"+sendKey+" value :"+sendVal);


                cursor.moveToNext();

            }

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return toSend;

    }

    private class ServerTask extends AsyncTask<ServerSocket, Void, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            Log.e(TAG, "About to enter the server receive block");
            //my code from PA1
            try {
                Socket inp = null;
                while (true) {
                    inp = serverSocket.accept();

                    Log.d(TAG,"isRecoved :"+isRecovered);

                    while(!isRecovered){


                    }
                    Log.d(TAG,"isRecoved :"+isRecovered);
                    Log.d(TAG, "ServerTask::Received input");
                    ObjectInputStream input = new ObjectInputStream(inp.getInputStream());

                    ArrayList<String> messageList = (ArrayList<String>) input.readObject();
                    Log.d(TAG, "ServerTask::Received input messagelist: " + messageList.toString());


                    if (messageList.get(0).equals("Insert_First")) {

                        Log.d(TAG, "ServerTask: InsertFirst entered");
                        ArrayList<String> toSend = new ArrayList<>();

                        ContentValues cvalues = new ContentValues();
                        cvalues.put("key", messageList.get(4));
                        cvalues.put("value", messageList.get(5));

                        database.insert(Sqlhelper.TABLE_NAME, null, cvalues);
                        Log.d(TAG, "ServerTask: inserted in database Cvalues= " + cvalues);


                        toSend.add("Insert_Complete");
                        toSend.add(messageList.get(6));
                        toSend.add(messageList.get(4));
                        toSend.add(portStr);

                        ObjectOutputStream out = new ObjectOutputStream(inp.getOutputStream());
                        out.writeObject(toSend);
                        Log.d(TAG,"ServerTask: Insert_First: toSend :"+toSend.toString());
                        Log.d(TAG,"ServerTask: Insert_First: sent out :"+out.toString());


                        Log.d(TAG, "ServerTask: calling Clienttask with message: " + messageList.toString());
                        //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, tosend);

                        out.flush();
                        input.close();
                        out.close();
                        inp.close();
                    }
                    else if (messageList.get(0).equals("Query_Key")) {

                        Log.d(TAG, "ServerTask: Query_Key: messagelist : " + messageList.toString());
                        ArrayList<String> toSend = new ArrayList<>();
                        String keyQuery = messageList.get(2);
                        String queryOrigin = messageList.get(3);
                        Log.d(TAG, "ServerTask: Query_Key: calling query() with key: " + keyQuery + "origin: " + queryOrigin);

                        toSend=QueryMyDatabaseAndSendPacket(uri, null, keyQuery, queryOrigin);

                        ObjectOutputStream out = new ObjectOutputStream(inp.getOutputStream());
                        out.writeObject(toSend);
                        Log.d(TAG,"ServerTask: Insert_First: toSend :"+toSend.toString());
                        Log.d(TAG,"ServerTask: Insert_First: sent out :"+out.toString());

                        out.flush();
                        input.close();
                        out.close();
                        inp.close();


                        Log.d(TAG, "Leaving ServerTask: Query_Key" + messageList.toString());

                    }
                    else if (messageList.get(0).equals("QueryKeyFailedMode")) {

                        Log.d(TAG, "ServerTask: Query_Key: messagelist : " + messageList.toString());
                        ArrayList<String> toSend = new ArrayList<>();
                        String keyQuery = messageList.get(2);
                        String queryOrigin = messageList.get(3);
                        Log.d(TAG, "ServerTask: Query_Key: calling query() with key: " + keyQuery + "origin: " + queryOrigin);

                        toSend=QueryMyDatabaseAndSendPacket(uri, null, keyQuery, queryOrigin);

                        ObjectOutputStream out = new ObjectOutputStream(inp.getOutputStream());
                        out.writeObject(toSend);

                        Log.d(TAG,"ServerTask: Insert_First: toSend :"+toSend.toString());
                        Log.d(TAG,"ServerTask: Insert_First: sent out :"+out.toString());

                        out.flush();
                        input.close();
                        out.close();
                        inp.close();


                        Log.d(TAG, "Leaving ServerTask: Query_Key" + messageList.toString());

                    }
                     else if (messageList.get(0).equals("Query_Star")) {

                        Log.d(TAG, "ServerTask: Query_Star: messagelist : " + messageList.toString());

                        ArrayList<String> toSend = new ArrayList<>();
                        String keyQuery = messageList.get(2);
                        String queryOrigin = messageList.get(3);

                        Log.d(TAG, "ServerTask: Query_Star: calling query() with key: " + keyQuery + "origin: " + queryOrigin);

                        Log.d(TAG, "Leaving ServerTask: Query_Star");

                        toSend=bundleQueryStarAndSend(queryOrigin);

                        ObjectOutputStream out = new ObjectOutputStream(inp.getOutputStream());
                        out.writeObject(toSend);
                        Log.d(TAG,"ServerTask: Insert_First: toSend :"+toSend.toString());
                        Log.d(TAG,"ServerTask: Insert_First: sent out :"+out.toString());

                        out.flush();
                        input.close();
                        out.close();
                        inp.close();

                    }
                    else if(messageList.get(0).equals("RecoverMode")){

                        Log.d(TAG,"ServerTask: Recovery Mode :with messageList :"+messageList.toString());

                        List<String> toSend = new ArrayList<>();

                        toSend.add(portStr+";RecoveryReceived");
                        Log.d(TAG,"added port :"+portStr);
                        toSend = getRecoveredEntries(messageList);
                        Log.d(TAG, " Recovered Entries :"+toSend.toString());

                        String destinationPort = messageList.get(1);
                        ObjectOutputStream out = new ObjectOutputStream(inp.getOutputStream());
                        out.writeObject(toSend);
                        Log.d(TAG,"ServerTask : RecoveryMode : out sent...Leaving ServerTask");

                        out.flush();
                        input.close();
                        out.close();
                        inp.close();


                    }
                    else if (messageList.get(0).equals("Delete_Star")){

                        int del = database.delete(Sqlhelper.TABLE_NAME, "1", null);

                    }
                    else if(messageList.get(0).equals("Delete_Key")){

                        String selection = messageList.get(4);
                        int del = database.delete(Sqlhelper.TABLE_NAME, Sqlhelper.Keyval + "=?", new String[]{selection});

                    }
                }
            } catch (IOException e) {
                Log.e(TAG, "error");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            //my code ends
            return null;
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private void printMatrixCursor(MatrixCursor matrixCursor) {

        matrixCursor.moveToFirst();
        while (matrixCursor.isAfterLast() == false) {
            String data = matrixCursor.getString(matrixCursor.getColumnIndex("key"));
            String col = matrixCursor.getString(matrixCursor.getColumnIndex("value"));
             Log.d(TAG, "key: " + data + " val " + col);

            matrixCursor.moveToNext();
        }
        Log.d(TAG, "Final matrix cursor");
        Log.d(TAG, "matrixcount=" + matrixCursor.getCount());

    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
