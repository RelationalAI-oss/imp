module TestData

using Base.Test

using Imp.Data

@testset "Data" begin

  @testset "quicksort!" begin
    for i in 1:10000
      srand(i)
      x = rand(Int, i)
      sx = sort(copy(x))
      Data.quicksort!((x,))
      @test x == sx
    end
  end
 
  @testset "merge - all keys match" begin
    for i in 1:10000
      srand(i)
      x = unique(rand(1:i, i))
      y = rand(1:i, length(x))
      z = rand(1:i, length(x))
      a = create_relation((x,y), 1)
      b = create_relation((x,z), 1)
      c = merge(a,b)
      @test get_rel_columns(c) == get_rel_columns(b)
    end
  end

  @testset "merge - no keys match" begin
    for i in 1:10000
      srand(i)
      x = unique(rand(1:i, i))
      x1 = [i*2 for i in x]
      x2 = [i*2+1 for i in x]
      y = rand(1:i, length(x))
      z = rand(1:i, length(x))
      a = create_relation((x1,y), 1)
      b = create_relation((x2,z), 1)
      c = merge(a,b)
      @test length(get_rel_column(c, 1)) == length(x1) + length(x2)
    end
  end

  @inferred create_relation(([1,2,3],["a","b","c"]), 1)

end

function bench()
  srand(999)
  x = rand(Int, 10000)
  # @show @benchmark quicksort!((copy($x),))
  
  srand(999)
  y = [string(i) for i in rand(Int, 10000)]
  # @show @benchmark quicksort!((copy($y),))
  
  srand(999)
  x = unique(rand(1:10000, 10000))
  y = rand(1:10000, length(x))
  z = rand(1:10000, length(x))
  a = create_relation((x,y), 1)
  b = create_relation((x,z), 1)
  # @show @benchmark merge($a,$b)
end

end
