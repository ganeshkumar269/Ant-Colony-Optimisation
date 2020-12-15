import java.util.*;
import java.io.*;
class temp{
    public static void main(String args[]) throws FileNotFoundException,IOException, ClassNotFoundException{
        ArrayList<Integer> arr = new ArrayList<Integer>();
        ArrayList<Double> wtList;
        ArrayList<String> aggList;
        wtList = new ArrayList<Double>(Arrays.asList(0.1417,0.1373,0.3481,0.964,0.325));
        aggList = new ArrayList<String>(Arrays.asList("sum","min","mul","mul","sum"));
        // for(int i =0 ;i < 10; i++)
        //     arr.add(i); 
        // Dataset dataset = new Dataset(10);
        // ArrayList<ArrayList<Double>> p = new ArrayList<ArrayList<Double>>();
        // for(int i =0 ;i < 10;i++){
        //     p.add(dataset.getRow(i,arr.get(i)));
        // }
        
        // Fitness2 fitness2 = new Fitness2(arr,aggList,wtList,dataset);
        // Fitness fitness = new Fitness(p,aggList,wtList);
        // System.out.println(fitness.calculate());
        // System.out.println(fitness2.calculate());
        FileOutputStream fos = new FileOutputStream("object.dat");
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(wtList);
        oos.close();
        FileInputStream fin = new FileInputStream("object.dat");
        ObjectInputStream ois = new ObjectInputStream(fin);
        ArrayList<Double> wtList2 = (ArrayList<Double>)ois.readObject();
        System.out.println(wtList2);
        System.out.println(wtList);
    
    }
}