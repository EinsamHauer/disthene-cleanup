package net.iponweb.disthene.cleanup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Andrei Ivanov
 */
public class PathNode {

    private String path;
    private boolean leaf;
    private Map<String, PathNode> children = new HashMap<>();

    public PathNode() {
    }

    public PathNode(String path, boolean leaf) {
        this.path = path;
        this.leaf = leaf;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public boolean isLeaf() {
        return leaf;
    }

    public void setLeaf(boolean leaf) {
        this.leaf = leaf;
    }

    public Map<String, PathNode> getChildren() {
        return children;
    }

    public int size() {
        int result = 1;

        for (PathNode node : children.values()) {
            result += node.size();
        }

        return result;

    }

    @Override
    public String toString() {
        return "PathNode{" +
                "path='" + path + '\'' +
                ", leaf=" + leaf +
                ", children=" + children +
                '}';
    }
}
