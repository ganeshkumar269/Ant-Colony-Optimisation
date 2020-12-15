// package hadoop;

import java.util.*;


public class Fitness2{
    private ArrayList<ArrayList<Double>> arr;
    private ArrayList<String>  aggFnList;
    private ArrayList<Double>  wtList;
    private ArrayList<Integer>  trail;
    private Dataset dataset;
    public Fitness2(){arr=null;}
    public Fitness2( 
                    ArrayList<Integer> _Arr,
                    ArrayList<String> _aggFnList,
                    ArrayList<Double>  _wtList,
                    Dataset _dataset
                )
    {
        trail = _Arr;
        aggFnList = _aggFnList;
        wtList = _wtList;
        dataset = _dataset;
    }

    private double getSum(int index){
        double sum = 0;
        for(int i = 0; i < trail.size(); i++)
            sum += dataset.getItem(i,trail.get(i),index);
        return sum;
    }

    private double getMul(int index){
        double mul = 1;
        for(int i = 0; i < trail.size(); i++)
            mul *= dataset.getItem(i,trail.get(i),index);
        return mul;
    }
    private double getMin(int index){
        double _min = Double.MAX_VALUE;
        for(int i = 0; i < trail.size(); i++)
            _min =  Math.min(dataset.getItem(i,trail.get(i),index),_min);
        return _min;
    }
    private double getMax(int index){
        double _max = Double.MIN_VALUE;
        for(int i = 0; i < arr.size(); i++)
            _max =  Math.max(dataset.getItem(i,trail.get(i),index),_max);
        return _max;
    }

    public double calculate(){
        double fitness = 0;
        for(int i = 0; i < 5; i++){
            switch (aggFnList.get(i)) {
                case "sum": fitness += wtList.get(i)*getSum(i);
                    break;
                case "mul": fitness += wtList.get(i)*getMul(i);
                    break;
                case "min": fitness += wtList.get(i)*getMin(i);
                    break;
                case "max": fitness += wtList.get(i)*getMax(i);
                    break;
                default:
                    break;
            }
        }
        return fitness;
    }
}