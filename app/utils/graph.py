from typing import List, Tuple

import numpy as np


class Graph:
    def __init__(
        self, edgelist: List[Tuple[int, int]], directed: bool = True
    ) -> None:
        self.edgelist = edgelist
        self.directed = directed
        self.nodes = self._init_node_set()
        self.adjacency_matrix = self._make_adjacency_matrix()

    def _init_node_set(self) -> List[int]:
        nodes = set()
        for e in self.edgelist:
            nodes.add(e[0])
            nodes.add(e[1])
        return sorted(list(nodes))

    def _make_adjacency_matrix(self) -> np.ndarray:
        a = np.zeros((self.num_nodes, self.num_nodes), dtype=np.int64)
        for e in self.edgelist:
            e_indices = (
                self.node_label_to_index(e[0]),
                self.node_label_to_index(e[1]),
            )
            a[e_indices[0], e_indices[1]] = 1
            if not self.directed:
                a[e_indices[1], e_indices[0]] = 1
        return a

    def node_index_to_label(self, node_index: int) -> int:
        return self.nodes[node_index]

    def node_label_to_index(self, node: int) -> int:
        return self.nodes.index(node)

    @property
    def num_nodes(self) -> int:
        return len(self.nodes)

    @property
    def num_edges(self) -> int:
        return len(self.edgelist)

    def bfs(self, root_node: int) -> List[int]:
        visited = [False] * self.num_nodes
        start_node = self.node_label_to_index(root_node)
        queue = [start_node]
        visited[start_node] = True
        bfs_order = []
        while queue:
            node = queue.pop(0)
            bfs_order.append(self.node_index_to_label(node))
            for i in range(self.num_nodes):
                if self.adjacency_matrix[node, i] == 1 and not visited[i]:
                    queue.append(i)
                    visited[i] = True
        return bfs_order
