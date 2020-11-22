import java.util.*;

class temp {
    public static void main(String... args){

        ArrayList<Double> prob = new ArrayList<Double>(Arrays.asList(0.3,0.3,0.3,0.1)); 
        ArrayList<Double> temp = new ArrayList<Double>(prob);
        double tempsum = temp.get(0);
        for(int i = 1;i < temp.size();i++){
            double currVal = temp.get(i);
            temp.set(i,1-tempsum);
            tempsum += currVal;
        }
        temp.set(0,1d);

        double randVal = Math.random();
        double selVal = temp.size()-1;
        for(int i = 0; i < temp.size()-1;i++){
            if(randVal <= temp.get(i) && randVal > temp.get(i+1)){
                selVal = i;
                break;
            }
        }
        System.out.println(selVal);

    }
}