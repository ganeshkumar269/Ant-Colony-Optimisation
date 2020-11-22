import com.opencsv.CSVReader;
import java.io.*;
import java.util.*;





class test{
    public static void main(String... args){
        Dataset dataset = new Dataset();
        System.out.println(dataset.getItemsPerTask());
        System.out.println(dataset.getItem(0,3,1));
    }
}