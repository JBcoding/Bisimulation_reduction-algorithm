import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Driver {

    private static final Object monitor = new Object();

    public static void main(String[] args) throws SQLException {
        if (args.length < 2) {
            return;
        }

        int blockCount = Integer.parseInt(args[0]);
        int maxThreads = Integer.parseInt(args[1]);


        Connection conn = null;
        Properties connectionProps = new Properties();
        connectionProps.put("user", "root");
        connectionProps.put("password", "");

        conn = DriverManager.getConnection(
                "jdbc:mysql://35.198.146.222" + ":" + 3306 + "/v1", connectionProps);

        System.out.println(System.nanoTime());
        try {
            long t = System.nanoTime();

            final int[] nextLabel = {0};

            System.out.println("Started downloading");

            List<Vertex> vertices = new ArrayList<>();
            List<Edge> edges = new ArrayList<>();

            Statement st = conn.createStatement();
            // I know prepared statements would be better, but this is only me using this on a tester server :)
            ResultSet rs = st.executeQuery("SELECT * FROM Vertices WHERE block_height >= 474044 - " + blockCount + ";");

            int maxId = 0;

            while (rs.next()) {
                vertices.add(new Vertex(
                        rs.getInt("id"),
                        rs.getString("address"),
                        rs.getInt("block_height"),
                        rs.getInt("label_last_full_step")));
            }

            Map<Integer, Vertex> idToVertexMap = new HashMap<>();
            for (Vertex v : vertices) {
                maxId = Math.max(maxId, v.getId());
                idToVertexMap.put(v.getId(), v);
                nextLabel[0] = Math.max(nextLabel[0], v.getLabelLastFullStep() + 1);
            }

            System.out.println("Downloaded vertices got " + vertices.size() + " vertices, starting edges");

            rs = st.executeQuery("SELECT * FROM Edges WHERE in_id <= " + maxId + " AND out_id <= " + maxId + ";");

            while (rs.next()) {
                int inId = rs.getInt("in_id");
                int outId = rs.getInt("out_id");
                if (idToVertexMap.containsKey(outId) && idToVertexMap.containsKey(inId)) {
                    idToVertexMap.get(outId).addParent(idToVertexMap.get(inId));
                    edges.add(new Edge(inId, outId));
                }
            }

            System.out.println("Downloaded edges got " + edges.size() + " edges");

            Map<Integer, Map<Integer, Vertex>> verticesMapLabelLastFullStep = new HashMap<>();
            for (Vertex v : vertices) {
                if (!verticesMapLabelLastFullStep.containsKey(v.getLabelLastFullStep())) {
                    verticesMapLabelLastFullStep.put(v.getLabelLastFullStep(), new HashMap<>());
                }
                verticesMapLabelLastFullStep.get(v.getLabelLastFullStep()).put(v.getId(), v);
            }
            Map<Integer, Map<Integer, Vertex>> verticesMapLabelCurrent = new ConcurrentHashMap<>();
            for (Integer label : verticesMapLabelLastFullStep.keySet()) {
                verticesMapLabelCurrent.put(label, new HashMap<>(verticesMapLabelLastFullStep.get(label)));
            }

            System.out.println("Downloaded data: " + (System.nanoTime() - t) / 1e9 + " s");
            t = System.nanoTime();

            int iteration = 0;
            Map<Integer, Integer> labelsSizeLastTimeItWasUsed = new HashMap<>();
            while (true) {
                iteration ++;
                System.out.println("Iteration: " + iteration);
                int oldNextLabel = nextLabel[0];
                for (Integer superGroup : verticesMapLabelLastFullStep.keySet()) {
                    if (labelsSizeLastTimeItWasUsed.containsKey(superGroup) &&
                            labelsSizeLastTimeItWasUsed.get(superGroup) == verticesMapLabelLastFullStep.get(superGroup).size()) {
                        continue;
                    }
                    labelsSizeLastTimeItWasUsed.put(superGroup, verticesMapLabelLastFullStep.get(superGroup).size());
                    Queue<Integer> groups = new ConcurrentLinkedQueue<>(verticesMapLabelCurrent.keySet());
                    final int[] threadsRunning = {0};
                    for (final int[] threadCount = {0}; threadCount[0] < maxThreads; threadCount[0]++) {
                        synchronized (monitor) {
                            threadsRunning[0]++;
                        }
                        new Thread(new Runnable() {
                            @Override
                            public void run() {
                                while (true) {
                                    Integer group = groups.poll();
                                    if (group == null) {
                                        break;
                                    }
                                    Set<Vertex> topDown = new HashSet<>(verticesMapLabelCurrent.get(group).values());
                                    Set<Vertex> bottomUp = new HashSet<>();
                                    for (Map.Entry<Integer, Vertex> v : verticesMapLabelCurrent.get(group).entrySet()) {
                                        if (v.getValue().hasParentInSuperGroup(superGroup)) {
                                            topDown.remove(v.getValue());
                                            bottomUp.add(v.getValue());
                                        }
                                    }
                                    if (topDown.size() != 0 && bottomUp.size() != 0) {
                                        // split is needed
                                        Set<Vertex> toBeRemoved = topDown;
                                        if (bottomUp.size() < topDown.size()) {
                                            toBeRemoved = bottomUp;
                                        }

                                        Map<Integer, Vertex> newMap = new HashMap<>();
                                        int localNextLabel;
                                        synchronized (monitor) {
                                            localNextLabel = nextLabel[0];
                                            nextLabel[0]++;
                                        }
                                        verticesMapLabelCurrent.put(localNextLabel, newMap);
                                        Map oldMap = verticesMapLabelCurrent.get(group);
                                        for (Vertex v : toBeRemoved) {
                                            v.setLabelCurrent(localNextLabel);
                                            oldMap.remove(v.getId());
                                            newMap.put(v.getId(), v);
                                        }
                                    }
                                }
                                synchronized (monitor) {
                                    threadsRunning[0] --;
                                    monitor.notifyAll();
                                }
                            }
                        }).start();
                    }
                    synchronized (monitor) {
                        while (threadsRunning[0] != 0) {
                            try {
                                monitor.wait();
                            } catch (InterruptedException e) {
                            }
                        }
                    }
                }
                for (Vertex v : vertices) {
                    v.updateLabelLastFullStep();
                }
                verticesMapLabelLastFullStep.clear();
                for (Integer label : verticesMapLabelCurrent.keySet()) {
                    verticesMapLabelLastFullStep.put(label, new HashMap<>(verticesMapLabelCurrent.get(label)));
                }

                System.out.println("Iteration " + iteration + ": " + (System.nanoTime() - t) / 1e9 + " s, #Labels: " + (nextLabel[0] - 1));
                t = System.nanoTime();

                if (nextLabel[0] == oldNextLabel) {
                    break;
                }
            }

            Set<Long> newEdges = new HashSet<>();
            for (Edge e : edges) {
                newEdges.add(4294967296L * idToVertexMap.get(e.getFrom()).getLabelLastFullStep() + idToVertexMap.get(e.getTo()).getLabelLastFullStep());
            }

            System.out.println("Total number of edges: " + newEdges.size());

            /*
            System.out.println("Started uploading");
            t = System.nanoTime();

            conn.setAutoCommit(false);
            PreparedStatement ps = conn.prepareStatement("UPDATE Vertices SET label_current = ? WHERE id = ?;");

            for (int i = 0; i < vertices.size(); i ++) {
                Vertex v = vertices.get(i);
                ps.setInt(1, v.getLabelLastFullStep());
                ps.setInt(2, v.getId());
                ps.addBatch();
                if (i % 1000 == 0) {
                    ps.executeBatch();
                }
            }
            if (vertices.size() % 1000 != 0) {
                ps.executeBatch();
            }

            conn.commit();

            System.out.println("Uploaded data: " + (System.nanoTime() - t) / 1e9 + " s");
            */


        } finally {
            conn.close();
        }
        System.out.println(System.nanoTime());
    }
}
