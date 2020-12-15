import java.util.*;

class temp{
    public static void main(String args[]){
        Dataset dataset = new Dataset
        ArrayList<Integer> arr = new ArrayList<Integer>();
        ArrayList<Double> wtList;
        ArrayList<String> aggList;
        wtList = new ArrayList<Double>(Arrays.asList(0.1417,0.1373,0.3481,0.964,0.325));
        aggList = new ArrayList<String>(Arrays.asList("sum","min","mul","mul","sum"));
        for(int i =0 ;i < 10; i++)
            arr.add(i); 
        Dataset dataset = new Dataset(10);
        Fitness fitness = new Fitness(arr,aggList,wtList,dataset);
        System.out.println(fitness.calculate());
    }
}