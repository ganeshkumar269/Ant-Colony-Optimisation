import com.opencsv.CSVReader;
import java.io.*;
import java.util.*;





class test{
    public static void main(String... args){
        int taskNum = 20;
        int numOfAnts = 10;
        ACO2 aco = new ACO2(taskNum);
        ArrayList<Ant2> ants = new ArrayList<Ant2>();
        aco.setNumOfItr(3);
        aco.setNumOfAnts((int)(200));
        // aco.setAnts();
        double bestFitnessSoFar = -1;
        for(int i = 0;i < numOfAnts; i++)
            ants.add(new Ant2(taskNum,aco.cost,aco.pher,aco.wtList,aco.aggList,aco.dataset));
        long startTime = System.currentTimeMillis();
        for(int i = 0;i < 10;i++){
            for(int j = 0; j < numOfAnts; j++){
                ants.get(j).move();
            }
            aco.evaporate();
            for(int j = 0; j < numOfAnts; j++){
                double antFN = ants.get(j).getFitness();
                bestFitnessSoFar = Math.max(bestFitnessSoFar,antFN);
                System.out.println("Ant "+ j + " fitness " + antFN);
                ants.get(j).updateTrail();
            }
            System.out.println("Iteration "+ i + "Best Fitness So Far " + bestFitnessSoFar);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Time Taken: " + (endTime-startTime) + "ms");
        // aco.printMetrics();
        
    }
}