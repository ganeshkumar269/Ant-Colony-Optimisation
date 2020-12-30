import java.io.*;
import java.util.*;

class Gen{
    public static void main(String args[]) throws FileNotFoundException,IOException{
        BufferedReader bf = new BufferedReader(new FileReader("dataset.txt"));
        PrintWriter pw = new PrintWriter("sample2i.txt");
        String line = bf.readLine();

        int taskNum = 10;
        int rowsPerTask = 2500/taskNum;
        int cnt = 0;
        while((line = bf.readLine()) != null){
            int task = cnt/rowsPerTask;
            StringBuilder qwsData = new StringBuilder();
            StringBuilder qwsData2 = new StringBuilder();
            StringTokenizer tokenizer = new StringTokenizer(line,",");
            ArrayList<String> vals = new ArrayList<String>();
            ArrayList<Double> valsD = new ArrayList<Double>();
            while(tokenizer.hasMoreTokens())
               vals.add(tokenizer.nextToken());  

            valsD.add(Double.parseDouble(vals.get(6)));
            valsD.add(Double.parseDouble(vals.get(2)));
            valsD.add(Double.parseDouble(vals.get(1)));
            valsD.add(Double.parseDouble(vals.get(4)));
            valsD.add(Double.parseDouble(vals.get(0)));

            double factor = (Math.random()-0.5);
            for(int i = 0;i < valsD.size(); i++){
                double d = valsD.get(i);
                d = d + factor*d;
                if(d <= 0) d = 0.0001;
                if(d >= 1) d = 0.9999;
                valsD.set(i,d);
            }
            //1582
            StringTokenizer IDT = new StringTokenizer(vals.get(9)," ");
            String ID = IDT.nextToken();
            qwsData = qwsData.append(String.valueOf(task) + " ");
            qwsData = qwsData.append(ID + " ");
            qwsData = qwsData.append(vals.get(6) + " ");   
            qwsData = qwsData.append(vals.get(2) + " ");   
            qwsData = qwsData.append(vals.get(1) + " ");   
            qwsData = qwsData.append(vals.get(4) + " ");   
            qwsData = qwsData.append(vals.get(0) + " ");
            qwsData2 = qwsData2.append(String.valueOf(task) + " ");
            qwsData2 = qwsData2.append(ID + "D ");
            qwsData2 = qwsData2.append(String.valueOf(valsD.get(0)) + " ");   
            qwsData2 = qwsData2.append(String.valueOf(valsD.get(1)) + " ");   
            qwsData2 = qwsData2.append(String.valueOf(valsD.get(2)) + " ");   
            qwsData2 = qwsData2.append(String.valueOf(valsD.get(3)) + " ");   
            qwsData2 = qwsData2.append(String.valueOf(valsD.get(4)) + " ");   
            pw.write(qwsData.toString());
            pw.write("\n");
            cnt++;
        }
        pw.flush();
    }
}