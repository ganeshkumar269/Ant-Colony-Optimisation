import java.util.*;

class printer{
    public void print(){System.out.println(temp.val);}
}

class temp{
    public static ArrayList<Integer> arr = new ArrayList<Integer>(); 
    public static int val = 10;
    public static void main(String args[]){
        printer p = new printer();
        p.print();
    }
}