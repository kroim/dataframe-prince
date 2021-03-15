import heapq
from collections import defaultdict

global shortestPath
global shortestCost
shortestPath = "null"
shortestCost = 100000


def dijkstra(graph, src, dest):
    h = []
    # keep a track record of vertices with cost
    # heappop will return vertex with least cost
    # greedy SRC -> MIN -> MIN -> MIN -> DEST

    heapq.heappush(h, (0, src))
    global shortestPath
    global shortestCost
    global source
    while len(h) != 0:
        currcost, currvtx = heapq.heappop(h)
        if currvtx == dest:
            print("\t\t\t{} to {} with cost {}".format(src, dest, currcost))
            if currcost < shortestCost:
                if dest == source:
                    continue
                else:
                    shortestCost = currcost
                    shortestPath = dest
            break
        for neigh, neighcost in graph[currvtx]:
            heapq.heappush(h, (currcost + neighcost, neigh))


city = ["Kajang", "Bangi", "Klang", "Ipoh", "Johor", "Seremban"]
graph = defaultdict(list)  # create a graph
v, e = map(int, input().split())
for i in range(e):
    u, v, w = map(str, input().split())
    graph[u].append((v, int(w)))

print("\t=============================================================================")
print("\t                     Dijkstra's Shortest Path Algorithm")
print("\t=============================================================================")
print("\n\tAssume 1 cost = 10 km")
print("\n\tCity availability: {} ".format(city))
global source
src = str(input("\n\tPlease enter your starting point of city :"))
while (src != "Kajang") and (src != "Bangi") and (src != "Klang") and (src != "Ipoh") and (src != "Johor") and (src != "Seremban"):
    src = str(input("\n\tInvalid choice. Please enter another city :"))
source = src
print("\n\tPossible paths from {} :\n".format(src))
for i in city:
    dijkstra(graph, src, i)
print("\n\t=============================================================================")
print("\t\tThe shortest path from {} is {} to {} with cost {}".format(src, src, shortestPath, shortestCost))
print("\t=============================================================================")