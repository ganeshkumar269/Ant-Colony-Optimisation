import java.util.*;

public class Ant{
    private ArrayList<Integer> trail;
    private int state;

    public Ant(int taskNum){
        trail = new ArrayList<Integer>();
        for(int i = 0;i < taskNum; i++)
            trail.add(0);
        state = 0;
    }

    public void visit(int task, int value) {
        // System.out.println(task + " " + trail.size());
        trail.set(task, value);
        state = value;
    }

    public int getState(){return state;}
    public ArrayList<Integer> getTrail(){return trail;}

    public void setState(int _state){state = _state;}
}