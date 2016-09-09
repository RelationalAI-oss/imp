include("src/Data.jl")
include("src/Query.jl")
include("src/UI.jl")

Data.test()

include("examples/JobData.jl")

include("examples/Graph.jl")
include("examples/Chinook.jl")
include("examples/Job.jl")
include("examples/Minesweeper.jl")

Graph.test()
Chinook.test()
Job.test()

Data.bench()

Graph.bench()
Chinook.bench()
Job.bench()
