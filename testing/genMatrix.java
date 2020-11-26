import java.io.*;
import java.util.*;
import hadoop.Dataset;

class genMatrix{
    public static Dataset dataset;
    public static ArrayList<ArrayList<ArrayList<Double>>> cost;
    public static ArrayList<ArrayList<ArrayList<Double>>> pher;
    public static ArrayList<Double> wtList;
    public static ArrayList<String> aggList;
    public static int taskNum = 50;
    public static void main(String... args) throws FileNotFoundException, IOException{
        dataset = new Dataset(taskNum);
        wtList = new ArrayList<Double>(Arrays.asList(0.1417,0.1373,0.3481,0.964,0.325));
        aggList = new ArrayList<String>(Arrays.asList("sum","min","mul","mul","sum"));
        cost = new ArrayList<ArrayList<ArrayList<Double>>>();
        pher = new ArrayList<ArrayList<ArrayList<Double>>>();
        int numOfAnts = 10;
        if(args.length > 0)
            numOfAnts = Integer.parseInt(args[0]);
        generateMatrices();
        PrintWriter pw = new PrintWriter("data.txt");
        String qwsData = new String();
        BufferedReader br = new BufferedReader(new FileReader("qws2_csv_normalised_4.csv"));
        String nextline = br.readLine();   
        StringTokenizer tokenizer;

        while((nextline = br.readLine()) != null){
            tokenizer = new StringTokenizer(nextline,",");
            ArrayList<String> vals = new ArrayList<String>();
            while(tokenizer.hasMoreTokens())
                vals.add(tokenizer.nextToken());
            
            qwsData = qwsData + vals.get(6) + " ";   
            qwsData = qwsData + vals.get(2) + " ";   
            qwsData = qwsData + vals.get(1) + " ";   
            qwsData = qwsData + vals.get(4) + " ";   
            qwsData = qwsData + vals.get(0) + " ";

        }   

        for(int i = 0; i < numOfAnts; i++){
            pw.write(String.valueOf(i) + " ");
            pw.write(String.valueOf(taskNum) + " ");
            pw.write(String.valueOf(cost.get(1).size())+ " ");
            pw.write(String.valueOf(cost.get(1).size())+ " ");
    
            writeMatrix(pw,cost);
            writeMatrix(pw,pher);

            pw.write(qwsData);

            pw.write("\n");   
        }
        pw.flush();

        // String line = br.readLine();
        // StringTokenizer tokenizer = new StringTokenizer(line," "); 
        // String tokens[] = tokenizer.nextToken();
        // System.out.println(cost.size());
        // System.out.println(cost.get(1).size());
        // System.out.println(cost.get(1).get(1).size());
    }
    private static void writeMatrix(PrintWriter pw,ArrayList<ArrayList<ArrayList<Double>>> arr){
        for(int i = 0;i < arr.size();i++){
            for(int j = 0;j < arr.get(i).size(); j++){
                for(int k = 0; k < arr.get(i).get(j).size(); k++){
                    pw.write(String.valueOf( arr.get(i).get(j).get(k) ) + " ");
                }
            }
        }
    }
    private static void generateMatrices(){
        cost.add(generateCostMatrix(-1,1,dataset.getRowsPerTask()));
        for(int i = 0; i < taskNum-1; i++)
            cost.add(generateCostMatrix(i,dataset.getRowsPerTask(),dataset.getRowsPerTask()));
        
        pher.add(generatePherMatrix(-1,1,dataset.getRowsPerTask()));
        for(int i = 0; i < taskNum-1; i++)
            pher.add(generatePherMatrix(i,dataset.getRowsPerTask(),dataset.getRowsPerTask()));
    }
    private static ArrayList<ArrayList<Double>> generateCostMatrix(int task,int r,int c){
        ArrayList<ArrayList<Double>> matrix = new ArrayList<ArrayList<Double>>();
        ArrayList<Double> tempRow;
        for(int i =0; i < r; i++){
            tempRow = new ArrayList<Double>();
            for(int j = 0; j < c;j++){
                tempRow.add(getDistance(task,i,j));
            }
            matrix.add(tempRow);
        }
        return matrix;
    }
    private static ArrayList<ArrayList<Double>> generatePherMatrix(int task,int r,int c){
        ArrayList<ArrayList<Double>> matrix = new ArrayList<ArrayList<Double>>();
        ArrayList<Double> tempRow;
        for(int i =0; i < r; i++){
            tempRow = new ArrayList<Double>();
            for(int j = 0; j < c;j++){
                tempRow.add(Math.random());
            } 
            matrix.add(tempRow);
        }
        return matrix;
    }
    private static double getDistance(int task,int i, int j){
        double dist = 0;
        if(task == -1)
            for(int attr = 0; attr < dataset.getItemsPerRow(); attr++)
                dist += wtList.get(attr)*dataset.getItem(0,j,attr);
        else
            for(int attr = 0; attr < dataset.getItemsPerRow(); attr++){
                switch(aggList.get(attr)){
                    case "sum": dist += wtList.get(attr)*(dataset.getItem(task,i,attr)+dataset.getItem(task+1,j,attr));break;
                    case "mul": dist += wtList.get(attr)*(dataset.getItem(task,i,attr)*dataset.getItem(task+1,j,attr));break;
                    case "min": dist += wtList.get(attr)*Math.min(dataset.getItem(task,i,attr),dataset.getItem(task+1,j,attr));break;
                    case "max": dist += wtList.get(attr)*Math.max(dataset.getItem(task,i,attr),dataset.getItem(task+1,j,attr));break;
                }
            }
        return dist;
    }
}