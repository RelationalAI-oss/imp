module TestCompiler

using Base.Test

using Imp.Data
using Imp.Compiler

@testset "Compiler" begin

  zz(x, y) = (x * x) + (y * y) + (3 * x * y)
  const xx = create_relation((collect(0:100),collect(0:100)))
  const yy = create_relation((collect(0:100), collect(reverse(0:100))))
  inputs = Dict(:xx => xx, :yy => yy, :zz => zz)

  expected = sum(((x * x) + (y * y) + (3 * x * y) for (x,y) in zip(get_rel_column(xx, 2), get_rel_column(yy, 2))))

  @testset "poly1" begin

    polynomial_ast1 = Lambda(
      :poly1,
      [],
      SumProduct(
        Ring{Int64}(+,*,1,0,nothing),
        [
          FunCall(:xx, typeof(xx), [:i, :x]),
          FunCall(:yy, typeof(yy), [:i, :y]),
          FunCall(zz, [:x, :y, :z]),
        ],
        [:z]
      )
    )  
    
    p1 = compile_function(polynomial_ast1)
    j1 = p1(inputs)
    @inferred j1()
    @test j1() == expected
    
  end
    
  @testset "poly2 - repeated variables" begin 
  
    polynomial_ast2 = Lambda(
      :poly2,
      [],
      SumProduct(
        Ring{Int64}(+,*,1,0,nothing),
        [
          FunCall(:xx, typeof(xx), [:x, :x]),
          FunCall(:yy, typeof(yy), [:x, :y]),
          FunCall(zz, [:x, :y, :z]),
        ],
        [:z]
      )
    )
    
    p2 = compile_function(polynomial_ast2)  
    j2 = p2(inputs)
    @inferred j2()
    @test j2() == expected
    
  end
  
  @testset "poly3 - constants" begin
  
    polynomial_ast3 = Lambda(
        :poly3,
        [],
        SumProduct(
          Ring{Int64}(+,*,1,0,nothing),
          [
            FunCall(:xx, typeof(xx), [:i, :x]),
            FunCall(:yy, typeof(yy), [:i, :y]),
            FunCall(*, [:x, :x, :t1]),
            FunCall(*, [:y, :y, :t2]),
            FunCall(*, [Constant(3), :x, :y, :t3]),
            FunCall(+, [:t1, :t2, :t3, :z])
          ],
          [:z]
        )
    )
    
    p3 = compile_function(polynomial_ast3)
    j3 = p3(inputs)
    @inferred j3()
    @test j3() == expected
    
  end
  
  @testset "poly4 - compile_function, 1 arg" begin

    polynomial_ast4 = Lambda(
        :poly4,
        [:i],
        SumProduct(
          Ring{Int64}(+,*,1,0,nothing),
          [
            FunCall(:xx, typeof(xx), [:i, :x]),
            FunCall(:yy, typeof(yy), [:i, :y]),
            FunCall(*, [:x, :x, :t1]),
            FunCall(*, [:y, :y, :t2]),
            FunCall(*, [Constant(3), :x, :y, :t3]),
            FunCall(+, [:t1, :t2, :t3, :z])
          ],
          [:z]
        )
    )

    p4 = compile_function(polynomial_ast4)
    @inferred p4(inputs)
    j4 = p4(inputs)
    @inferred j4(1)
    for x in 0:100
      y = 100-x
      z = (x * x) + (y * y) + (3 * x * y)
      @test j4(x) == z
    end
    
  end

  @testset "poly5 - compile_relation, 1 arg" begin
  
    polynomial_ast5 = Lambda(
        :poly4,
        [:i],
        SumProduct(
          Ring{Int64}(+,*,1,0,nothing),
          [
            FunCall(:xx, typeof(xx), [:i, :x]),
            FunCall(:yy, typeof(yy), [:i, :y]),
            FunCall(*, [:x, :x, :t1]),
            FunCall(*, [:y, :y, :t2]),
            FunCall(*, [Constant(3), :x, :y, :t3]),
            FunCall(+, [:t1, :t2, :t3, :z])
          ],
          [:z]
        )
    )
  
    p5 = compile_relation(polynomial_ast5)
    @inferred p5(inputs)
    j5 = p5(inputs)
    for x in 0:100
      y = 100-x
      z = (x * x) + (y * y) + (3 * x * y)
      @test get_rel_column(j5, 1)[x+1] == x
      @test get_rel_column(j5, 2)[x+1] == z
    end
    
  end
  
  @testset "poly6 - compile_relation, 0 args" begin
    
    polynomial_ast6 = Lambda(
        :poly3,
        [],
        SumProduct(
          Ring{Int64}(+,*,1,0,nothing),
          [
            FunCall(:xx, typeof(xx), [:i, :x]),
            FunCall(:yy, typeof(yy), [:i, :y]),
            FunCall(*, [:x, :x, :t1]),
            FunCall(*, [:y, :y, :t2]),
            FunCall(*, [Constant(3), :x, :y, :t3]),
            FunCall(+, [:t1, :t2, :t3, :z])
          ],
          [:z]
        )
    )
    
    p6 = compile_relation(polynomial_ast6)
    @inferred p6(inputs)
    j6 = p6(inputs)
    @test get_rel_column(j6, 1)[1] == expected

  end

end

# using BenchmarkTools
# const big_xx = create_relation((collect(0:1000000),collect(0:1000000)))
# const big_yy = create_relation((collect(0:1000000), collect(reverse(0:1000000))))
# big_inputs = Dict(:xx => big_xx, :yy => big_yy)
# 
# polynomial_ast5 = Lambda(
#     :poly4,
#     [:i],
#     SumProduct(
#       Ring{Int64}(+,*,1,0,nothing),
#       [
#         FunCall(:xx, typeof(big_xx), [:i, :x]),
#         FunCall(:yy, typeof(big_yy), [:i, :y]),
#         FunCall(*, [:x, :x, :t1]),
#         FunCall(*, [:y, :y, :t2]),
#         FunCall(*, [Constant(3), :x, :y, :t3]),
#         FunCall(+, [:t1, :t2, :t3, :z])
#       ],
#       [:z]
#     )
# )
# 
# p5 = compile_relation(polynomial_ast5)
# 
# @show @benchmark p5(big_inputs)

end
