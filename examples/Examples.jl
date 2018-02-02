module Examples

using Base.Test

include("LegacyParser.jl")
include("Chinook.jl")
include("Graph.jl")
include("Polynomial.jl")
include("JobData.jl") 
include("Job.jl")

function test()
  @testset "examples" begin
    Chinook.test()
    Graph.test()
    Job.test()
  end
end

end
