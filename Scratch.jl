include("src/Util.jl")
include("src/Data.jl")
include("src/Query.jl")
include("src/Flows.jl")
include("src/UI.jl")
# if isdefined(:Todo)
#   close(Todo.view)
# end
include("examples/Todo.jl")
# close(Todo.view)
UI.serve(Todo.view)
# include("src/Live.jl")
# Live.run("examples/Todo.jl")
# Live.todo()

end
