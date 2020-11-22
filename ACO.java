import java.util.*;

public class ACO{
    ArrayList<ArrayList<ArrayList<Double>>> cost;
    ArrayList<ArrayList<ArrayList<Double>>> pher;
    ArrayList<Double> wtList;
    ArrayList<String> aggList;
    Dataset dataset;
    Fitness fitness;
    ArrayList<Ant> ants;
    ArrayList<Integer> bestFitnessValTrail;
    private double bestFitnessVal;
    private double c;
    private double alpha;
    private double beta;
    private double evap;
    private double Q;
    private double antFactor;
    private double randFactor;
    private double numOfAnts;

    public int taskNum;
    public ACO(){
        taskNum = 10;
        init();
    }
    public ACO(int _taskNum){
        taskNum = _taskNum;
        init();
    }
    private void init(){
        cost = new ArrayList<ArrayList<ArrayList<Double>>>(); 
        pher = new ArrayList<ArrayList<ArrayList<Double>>>(); 
        wtList = new ArrayList<Double>(Arrays.asList(0.1417,0.1373,0.3481,0.964,0.325));
        aggList = new ArrayList<String>(Arrays.asList("sum","min","mul","mul","sum"));

        c = 1.0;
        alpha = 1.0;
        beta = 1.0;
        evap = 0.5;
        Q = 500;
        antFactor = 0.8;
        randFactor = 0.1;
        numOfAnts = 10;
        ants = new ArrayList<Ant>();
        for(int i = 0;i < numOfAnts;i++)
            ants.add(new Ant(taskNum));
        generateMatrices();
        dataset = new Dataset(taskNum);
    }

    public void setTaskNum(int _taskNum){taskNum = _taskNum;}

    private void generateMatrices(){
        cost.add(generateCostMatrix(-1,1,dataset.getItemsPerTask()));
        for(int i = 0; i < taskNum-1; i++)
            cost.add(generateCostMatrix(i,dataset.getItemsPerTask(),dataset.getItemsPerTask()));
        
        pher.add(generatePherMatrix(-1,1,dataset.getItemsPerTask()));
        for(int i = 0; i < taskNum-1; i++)
            pher.add(generatePherMatrix(i,dataset.getItemsPerTask(),dataset.getItemsPerTask()));
    }



    private ArrayList<ArrayList<Double>> generateCostMatrix(int task,int r,int c){
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


    private ArrayList<ArrayList<Double>> generatePherMatrix(int task,int r,int c){
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


    private double getDistance(int task,int i, int j){
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


    public void run(){
        for(int i = 0; i < 3; i++){
            moveAnts();
            updateTrails();
        }
    }

    private void moveAnts(){
        for(int ind = 0;ind < taskNum; ind++){
            for(Ant ant : ants){
                // int selection = makeSelection(arr.get(ind).get(ant.getState()));
                int selection = makeSelection(ind,ant.getState());
                ant.visit(ind,selection);
            }
        }
        for(Ant ant : ants) ant.setState(0);

    }

    private void updateTrails(){
        //Evaporation
        for(int i =0 ;i < pher.size();i++)
            for(int j = 0;j < pher.get(i).size();j++)
                for(int k = 0; k < pher.get(i).get(j).size();k++)
                    pher.get(i).get(j).set(k,pher.get(i).get(j).get(k)*evap);


        
        for(Ant ant : ants){
            ArrayList<ArrayList<Double>> path;
            ArrayList<Integer> trail = ant.getTrail();
            for(int ind =0; ind < taskNum;ind++)
                path.add(dataset.getItem(ind,trail.get(ind)));
            fitness = new Fitness(path,aggList,wtList);
            double fitnessVal = fitness.calculate(); 
            updateBest(trail,fitnessVal);
            double contrib = Q/fitnessVal;
            pher.get(0).get(0).set(i,pher.get(0).get(0).get(trail.get(0))+contrib);
            for(int i = 1; i < taskNum; i++){
                pher.get(i).get(trail.get(i-1))
                    .set(trail.get(i),
                    pher.get(i).get(trail.get(i-1)).get(trail.get(i))+contrib);
            }
        }
    }

    private void updateBest(ArrayList<Integer> trail,double val){
        if(bestFitnessVal < val){
            bestFitnessVal = val;
            bestFitnessValTrail = new ArrayList<Integer>(trail);
        }
    }

    private int makeSelection(int task,int state){

        // randomly choose if we should make a random decision
        if(Math.random() < randFactor){
            return int(Math.random()*double(dataset.getItemsPerRow()));
        }

        //Calculate Probablities
        ArrayList<Double> prob = new ArrayList<Double>();
        double pheromone = 0.0;
        for(int ch =0 ;ch < dataset.getItemsPerTask();ch++){
            pheromone +=    (
                            Math.pow(pher.get(task).get(ch),alpha)
                            * Math.pow(1.0/cost.get(task).get(ch),beta)
                            );
        }

        for(int ch =0 ;ch < dataset.getItemsPerTask();ch++){
            double numerator = Math.pow(pher.get(task).get(ch),alpha)
            * Math.pow(1.0/cost.get(task).get(ch),beta);
            prob.add(numerator/pheromone);
        }

        //making selection
        double total = 0;
        double randVal = Math.random();
        for(int i = 0; i < prob.size(); i++){
            total += prob.get(i);
            if (total >= randVal)
                return i;
        }
        return 0;
    }
}