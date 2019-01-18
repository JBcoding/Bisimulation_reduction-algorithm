import java.util.ArrayList;
import java.util.List;

public class Vertex {
    private int id;
    private String address;
    private int blockHeight;
    private int labelLastFullStep;
    private int labelCurrent;
    private List<Vertex> parents;

    public Vertex(int id, String address, int blockHeight, int labelLastFullStep) {
        this.id = id;
        this.address = address;
        this.blockHeight = blockHeight;
        this.labelLastFullStep = labelLastFullStep;
        this.labelCurrent = labelLastFullStep;
        //this.labelCurrent = labelCurrent;
        this.parents = new ArrayList<>();
    }

    public void setLabelCurrent(int labelCurrent) {
        this.labelCurrent = labelCurrent;
    }

    public void addParent(Vertex parent) {
        parents.add(parent);
    }

    public int getId() {
        return id;
    }

    public String getAddress() {
        return address;
    }

    public int getBlockHeight() {
        return blockHeight;
    }

    public int getLabelLastFullStep() {
        return labelLastFullStep;
    }

    public int getLabelCurrent() {
        return labelCurrent;
    }

    public List<Vertex> getParents() {
        return parents;
    }

    public boolean hasParentInSuperGroup(Integer superGroup) {
        return parents.stream().map(Vertex::getLabelLastFullStep).anyMatch(l -> l.equals(superGroup));
    }

    @Override
    public String toString() {
        return String.format("%s, %s, %s, %s, %s", id, address, blockHeight, labelLastFullStep, labelCurrent);
    }

    public void updateLabelLastFullStep() {
        labelLastFullStep = labelCurrent;
    }
}
