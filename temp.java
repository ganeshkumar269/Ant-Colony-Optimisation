import java.util.*;

class testing{
    int t;
    testing(){t=10;}
    public static int value = 10;
}

class Printer{
    public void print(testing val){System.out.println("Printer: "+val.t);}
}

class testing2{
    private testing val;
    private Printer p;
    testing2(){val = new testing();p=new Printer();}
    void init(){p.print(val);}
}

class temp {
    public static class customComp implements Comparator<ArrayList<Integer>>{
        public int compare(ArrayList<Integer> a, ArrayList<Integer> b){
            return a.get(1).compareTo(b.get(1));
        }
    }
    public static void main(String... args){
        ArrayList<Integer> arr = new ArrayList<Integer>();
        ArrayList<ArrayList<Integer>> arr2 = new ArrayList<ArrayList<Integer>>();
        for(int i = 0; i< 3; i++){
            arr = new ArrayList<Integer>();
            arr.add(i);
            arr.add((int)(Math.random()*100));
            arr2.add(arr);
        }
        for(ArrayList<Integer> i : arr2){
            for(int j : i)
                System.out.println(j);
        }
        Collections.sort(arr2,new customComp());
        for(ArrayList<Integer> i : arr2){
            for(int j : i)
                System.out.println(j);
        }
    }
}