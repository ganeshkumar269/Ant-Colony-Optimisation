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
    public static void main(String... args){
        System.out.println(testing.value);
    }
}