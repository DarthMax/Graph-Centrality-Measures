VERTICES = [1,2,3,4,5,6,7]
EDGES = [
  [1,2],
  [1,3],
  [3,4],
  [3,5],
  [5,7],
  [4,7],
  [4,6]
]

def neighbors(v)
    EDGES.select {|edge| edge[0]==v}.map {|edge| edge[1]}
end


centrality = Hash.new(0)

VERTICES.each do |s|
    stack = []
    predecessors = Hash.new {|h,k| h[k]=[]}

    sigma = Hash.new(0)
    sigma[s]=1

    d=Hash.new(-1)
    d[s]=0

    queue = [s]

    while !queue.empty? do
        v = queue.shift
        stack << v

        neighbors(v).each do |w|
            if d[w] < 0
                queue << w
                d[w] = d[v]+1
            end

            if d[w] == d[v]+1
                sigma[w] += sigma[v]
                predecessors[w] << v
            end
        end
    end


    delta = Hash.new(0)

    while !stack.empty? do
        w = stack.pop
        predecessors[w].each { |v2| delta[v2] += (sigma[v2]/sigma[w].to_f) * (1+delta[w]) }
        centrality[w] += delta[w] unless w==s
    end
end

centrality.each_pair do |v,c|
    puts "centrality of #{v} is #{c}"
end
