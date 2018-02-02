module Polynomial

using Imp.Data
using Examples.LegacyParser

using BenchmarkTools
using Base.Test

const xx = Relation((collect(1:1000000), collect(1:1000000)), 1)
const yy = Relation((collect(1:1000000), collect(1:1000000)), 1)

# have to put this in a separate function because LegacyParser is a bit crap
zz(x,y) = (x * x) + (y * y) + (3 * x * y)

@time function f(xx, yy) 
  @query begin
    xx(i, x)
    yy(i, y)
    zz(x, y, z)
    return (x::Int64, y::Int64, z::Int64)
  end
end

function bench()
  @show @benchmark f(xx, yy)
end

end
