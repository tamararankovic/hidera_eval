package main

import "slices"

type Graph struct {
	Adj [][]int `json:"edges"`
	Deg []int   `json:"degree"`
}

func BuildGraph(n, m int) *Graph {
	g := &Graph{
		Adj: make([][]int, n),
		Deg: make([]int, n),
	}

	for i := range n - 1 {
		g.addEdge(i, i+1)
	}

	for u := range n {
		for v := u + 2; v < n && g.Deg[u] < m; v++ {
			if g.Deg[v] < m && !g.isNeighbor(u, v) {
				g.addEdge(u, v)
			}
		}
	}

	return g
}

func (g *Graph) addEdge(u, v int) {
	g.Adj[u] = append(g.Adj[u], v)
	g.Adj[v] = append(g.Adj[v], u)
	g.Deg[u]++
	g.Deg[v]++
}

func (g *Graph) isNeighbor(u, v int) bool {
	return slices.Contains(g.Adj[u], v)
}
