import java.util.*;

public class Ant{
    private ArrayList<Integer> trail;
    private int state;

    public Ant(int taskNum){
        trail = new ArrayList<Integer>(taskNum);
        state = 0;
    }

    public void visit(int task, int value) {
        trail.set(task, value);
        state = value;
    }

    public int getState(){return state;}
    public ArrayList<Integer> getTrail(){return trail;}

    public void setState(int _state){state = _state;}
}