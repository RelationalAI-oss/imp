"""
Parser for legacy query syntax, to avoid having to rewrite Job.jl
"""

module LegacyParser

using Match

using Imp.Util
using Imp.Data
using Imp.Compiler

function parse_when(expr, mod::Module) ::FunCall
  # collect variables
  args = Union{Symbol, Constant}[]
  walk(expr) = @match expr begin
    _::Symbol => push!(args, expr)
    Expr(:call, [:in, var, set], _) => walk(var) 
    Expr(:call, [head, exprs...], _) => walk(exprs)
    Expr(:macrocall, [head, exprs...], _) => walk(exprs)
    Expr(:(&&), exprs, _) => walk(exprs)
    Expr(:(||), exprs, _) => walk(exprs)
    Expr(:comparison, [a, _, b, _, c], _) => walk([a,b,c])
    Expr(_, _, _) => error("Unknown @when syntax: $expr $(typeof(expr))")
    _::AbstractArray => foreach(walk, expr)
    _ => () # some constant
  end
  walk(expr)
  args = convert(typeof(args), unique(args))
  
  # make a closure that evalutes expr
  code = :(($(args...),) -> $expr)
  fun = eval(mod, code)
  
  FunCall(fun, typeof(fun), vcat(args, [Constant(true)]))
end

function parse_return(exprs, mod::Module) ::Vector{Symbol}
  vars = Symbol[]
  args = @match exprs begin
    [Expr(:tuple, args, _)] => args
    _ => error("Unknown return syntax: $args")
  end
  # strip optional type annotations
  for arg in args
    @match arg begin
      _::Symbol => push!(vars, arg)
      Expr(:(::), [symbol::Symbol, _], _) => push!(vars, symbol)
      _ => error("Unknown return arg syntax: $arg")
    end
  end
  vars
end

function parse_in(var, set, init, mod::Module) ::FunCall
  sym = gensym("fun")
  init[sym] = :(Relation((collect($set),), 1))
  # `x in xs` is only ever used with string columns 
  FunCall(sym, Relation{Tuple{Vector{String}}}, [var])
end

function parse_call(exprs, init, mod::Module) ::FunCall
  name = exprs[1]
  args = map(exprs[2:end]) do expr
    @match expr begin
      _::Symbol => expr == :(_) ? gensym(:(_)) : expr
      _::Union{Number, String} => Constant(expr)
      Expr(:call, _, _) => Constant(expr)
      _ => error("Unknown call arg syntax: $expr")
    end
  end
  fun = try
    eval(mod, name)
  catch _
    error("Fun is not defined: $name")
  end
  # TODO need to avoid differing representations here
  if isa(fun, Function)
    FunCall(fun, typeof(fun), args)
  else
    sym = gensym("fun")
    init[sym] = name
    FunCall(sym, typeof(fun), args)
  end
end

function parse_query(body, mod::Module) ::Tuple{Dict{Symbol, Any}, Lambda}
  init = Dict{Symbol, Any}()
  
  # just use bool ring
  ring = Ring(|, &, true, false, true)
  value = [Constant(true)]
  
  # either a multi-line or single-line query
  lines = @match body begin
    Expr(:block, lines, _) => lines
    line => [line]
  end
  
  # dispatch to parse_*
  args = Symbol[]
  domain = FunCall[]
  for line in lines
    @match line begin
      Expr(:macrocall, [head, expr], _) => @match head begin
        Symbol("@when") => @match expr begin
          Expr(:call, [:in, var, set], _) => push!(domain, parse_in(var, set, init, mod))
          _ => push!(domain, parse_when(expr, mod))
        end
        _ => error("Unknown macro: $head")
      end
      Expr(:return, exprs, _) => append!(args, parse_return(exprs, mod))
      Expr(:call, [:in, var, set], _) => push!(domain, parse_in(var, set, init, mod))
      Expr(:call, exprs, _) => push!(domain, parse_call(exprs, init, mod))
      Expr(:(=), [var, value], _) => push!(domain, parse_call([identity, value, var], init, mod))
      Expr(:line, _, _) => ()
      _ => error("Unknown query line syntax: $line")
    end
  end
  
  (init, Lambda(gensym("query"), args, SumProduct(ring, domain, value)))
end

macro query(body)
  mod = current_module()
  init, parsed = parse_query(body, mod)
  compiled = compile_relation(parsed)
  for call in parsed.body.domain
    if isa(call.name, Symbol)
      if !haskey(init, call.name)
        init[call.name] = call.name
      end
    end
  end
  :($compiled(Dict($(@splice (name, expr) in init :($(Expr(:quote, name)) => $(esc(expr)))))))
end

export @query

end
