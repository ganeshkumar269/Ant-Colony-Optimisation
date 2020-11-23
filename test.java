import com.opencsv.CSVReader;
import java.io.*;
import java.util.*;





class test{
    public static void main(String... args){
        ACO aco = new ACO(20);
        aco.setNumOfItr(10);
        aco.setNumOfAnts((int)(200));
        aco.setAnts();
        aco.run();
        aco.printMetrics();
    }
}