module Imp

import MacroTools
import MacroTools: @capture
using Rematch

# Abstract Interpreter Set type (used for input/intermediate/output data collections)
const ASet = Set
# Concrete Interpreter Set type (used for input/intermediate/output data collections)
const ISet = ASet
# Empty Interpreter Set type (used for input/intermediate/output data collections)
const ESet = ASet{Tuple{}}
# Singleton Interpreter Set type (used for input/intermediate/output data collections)
const SSet = ASet
# Type Set type
const TSet = Set
# Number Set type
const NSet = Set{Int64}

# TODO remove workaround for https://github.com/JuliaLang/julia/issues/26885
# function Base.show(io::IO, set::Set)
#     print(io, "Set(", collect(set), ")")
# end

############## START COMPATIBILITY WITH JULIA 0.7
function coalesce end
coalesce(x::Any) = x
# coalesce(x::Some) = x.value
coalesce(x::Void) = nothing
# coalesce(x::Missing) = missing
coalesce(x::Any, y...) = x
# coalesce(x::Some, y...) = x.value
# coalesce(x::Union{Void, Missing}, y...) = coalesce(y...)
coalesce(x::Void, y...) = coalesce(y...)
pairs(collection::Vector) = Base.Generator(=>, 1:length(collection), collection)
findall(testf::Function, A) = collect(first(p) for p in pairs(A) if testf(last(p)))
############## FINISH COMPATIBILITY WITH JULIA 0.7

macro splice(iterator, body)
  @assert iterator.head == :call
  @assert iterator.args[1] == :in
  Base.Expr(:..., :(($(esc(body)) for $(esc(iterator.args[2])) in $(esc(iterator.args[3])))))
end

# include("columns.jl")

# --- booleans

const false_set = ESet()
const true_set = SSet{Tuple{}}([()])
bool_to_set(bool::Bool)::ASet = bool ? true_set : false_set
set_to_bool(set::ASet)::Bool = !isempty(set)

# --- parse ---

abstract type Expr end

struct Constant <: Expr
    value::ASet # TODO ASet{T} where T <: Tuple
end

struct Var <: Expr
    name::Symbol
    scope::Int64 # vars with the same name in different scopes get different ids here
end

Var(name::Symbol) = Var(name, 0) # 0 == global scope

struct Apply <: Expr
    f::Expr
    args::Vector{Expr}
end

# things which operate on relations rather than on values
# TODO could be combined with higher order apply
struct Primitive <: Expr
    f::Symbol
    args::Vector{Expr}
end

struct Native <: Expr
    f::Function
    in_types::NTuple{N, Type} where N
    out_types::NTuple{N, Type} where N
end

struct Abstract <: Expr
    vars::Vector{Var}
    value::Expr
end

struct Let <: Expr
    var::Var
    value::Expr
    body::Expr
end

struct AbstractHigher <: Expr
    vars::Vector{Var}
    value::Expr
end

struct ApplyHigher <: Expr
    f::Expr
    args::Vector{Expr}
end

@generated function Base.:(==)(a::T, b::T) where {T <: Expr}
    Base.Expr(:&&, @splice fieldname in fieldnames(T) quote
         (a.$fieldname == b.$fieldname)
         end)
end

@generated function Base.hash(expr::Expr, h::UInt)
    value = :h
    for fieldname in fieldnames(expr)
        value = :(hash(expr.$fieldname, $value))
    end
    value
end

@generated function map_expr(f, constructor, expr::Expr)
    quote
        constructor($(@splice fieldname in fieldnames(expr) begin
                      if fieldtype(expr, fieldname) <: Expr
                      :(f(expr.$fieldname))
                      elseif fieldtype(expr, fieldname) <: Vector{T} where {T <: Expr}
                      :(map(f, expr.$fieldname))
                      else
                      :(expr.$fieldname)
                      end
                      end))
    end
end
map_expr(f, expr::Expr) = map_expr(f, typeof(expr), expr)

function parse(ast)
    if @capture(ast, bool_Bool)
        bool ? Constant(true_set) : Constant(false_set)
    elseif @capture(ast, constant_Int64_Float64_String)
        Constant(SSet([(constant,)]))
    elseif @capture(ast, name_Symbol)
        if name == :(_)
            Var(:everything)
        else
            Var(name)
        end
    elseif @capture(ast, f_(args__))
        if f in [:|, :&, :!, :(=>), :(==), :reduce, :exists, :forall]
            Primitive(f, map(parse, args))
        else
            Apply(parse(f), map(parse, args))
        end
    elseif @capture(ast, if cond_ true_branch_ end)
        Primitive(:iff, [parse(cond), parse(true_branch), Constant(false_set)])
    elseif @capture(ast, if cond_ true_branch_ else false_branch_ end)
        Primitive(:iff, [parse(cond), parse(true_branch), parse(false_branch)])
    elseif (ast isa Base.Expr) && (ast.head == :elseif)
        @match ast.args begin
            [cond, true_branch] => Primitive(:iff, [parse(cond), parse(true_branch), Constant(false_set)])
            [cond, true_branch, false_branch] => Primitive(:iff, [parse(cond), parse(true_branch), parse(false_branch)])
        end
    elseif @capture(ast, var_Symbol -> value_)
        Abstract([Var(var)], parse(value))
    elseif @capture(ast, (vars__,) -> value_)
        Abstract(map(Var, vars), parse(value))
    elseif @capture(ast, let begin bindings__ end; body_ end)
        reduce(parse(body), bindings) do body, binding
            @assert @capture(binding, var_Symbol = value_) "Unknown syntax: $ast"
            Let(Var(var), parse(value), body)
        end
    elseif @capture(ast, (exprs__,))
        Primitive(:tuple, map(parse, exprs))
    elseif @capture(ast, a_.b_)
        Primitive(:compose, [parse(a), parse(b)])
    elseif @capture(ast, {vars__} -> value_)
        AbstractHigher(map(Var, vars), parse(value))
    elseif @capture(ast, f_{args__})
        ApplyHigher(parse(f), map(parse, args))
    elseif @capture(ast, f_[ixes__])
        @assert all(ix -> ix isa Integer, ixes) "Unknown syntax: $ast"
        Permute(parse(f), ixes, [])
    elseif ast isa Expr
        # spliced in
        ast
    else
        error("Unknown syntax: $ast")
    end
end

function unparse_row(row::Tuple)
    @match length(row) begin
        0 => true
        1 => first(row)
        _ => row
    end
end

function unparse(expr::Expr)
    @match (expr, map_expr(unparse, tuple, expr)) begin
        (_::Constant, (value,)) => @match length(value) begin
            0 => false
            1 => unparse_row(first(value))
            _ => :(|($(map(unparse_row, value)...)))
        end
        (_::Var, (name, _)) => name
        (_::Apply, (f, args)) => :($f($(args...)))
        (_::Primitive, (:iff, [cond, true_branch, Constant(value)])) where (value == false_set) => :(if $cond; $true_branch end)
        (_::Primitive, (:iff, [cond, true_branch, false_branch])) => :(if $cond; $true_branch else $false_branch end)
        (_::Primitive, (:tuple, args)) => :(($(args...),))
        (_::Primitive, (:compose, [a, b])) => :($a.$b)
        (_::Primitive, (f, args)) => :($f($(args...),))
        (_::Abstract, ([var], value)) => :($var -> $value)
        (_::Abstract, (vars, value)) => :(($(vars...),) -> $value)
        (_::Let, (var, value, body)) => :(let $var = $value; $body end)
        (_::AbstractHigher, (vars, value)) => :({$(vars...),} -> $value)
        (_::ApplyHigher, (f, args)) => :($f{$(args...),})
    end
end

unparse(expr::Native) = :({Native($(expr.f), $(expr.in_types), $(expr.out_types))})

Base.show(io::IO, expr::Expr) = print(io, string("@imp(", repr(unparse(expr)), ")"))

# --- scoping ---

struct CompileError
    message::String
    stack_trace::Vector{StackTraces.StackFrame}
end

compile_error(message::String) = throw(CompileError(message, stacktrace()))

struct Scope
    current::Dict{Symbol, Int64}
    used::Dict{Symbol, Int64}
end

Scope() = Scope(Dict(), Dict())
Scope(env::Dict{Var}) = Scope(Dict(var.name => 0 for var in keys(env)), Dict(var.name => 0 for var in keys(env)))

separate_scopes(scope::Scope, expr::Expr) = map_expr((expr) -> separate_scopes(scope, expr), expr)

function separate_scopes(scope::Scope, expr::Var)
    id = get(scope.current, expr.name) do
        compile_error("Not in scope: $(expr.name)") # TODO how do we report location?
    end
    Var(expr.name, id)
end

function separate_scopes(scope::Scope, expr::Union{Abstract, AbstractHigher})
    scope = Scope(copy(scope.current), scope.used)
    for var in expr.vars
        scope.current[var.name] = scope.used[var.name] = get(scope.used, var.name, 0) + 1
    end
    map_expr((expr) -> separate_scopes(scope, expr), expr)
end

function separate_scopes(scope::Scope, expr::Let)
    # expr.var is not in scope during expr.value
    value = separate_scopes(scope, expr.value)
    scope = Scope(copy(scope.current), scope.used)
    scope.current[expr.var.name] = scope.used[expr.var.name] = get(scope.used, expr.var.name, 0) + 1
    var = separate_scopes(scope, expr.var)
    body = separate_scopes(scope, expr.body)
    Let(var, value, body)
end

# --- inline ---

function inline_let(expr::Expr)::Expr
    @match expr begin
        Let(var, value, body) => replace_expr(inline_let(body), Dict(var => inline_let(value)))
        _ => map_expr(inline_let, expr)
    end
end

function inline_higher(expr::Expr)::Expr
    while true
        expr = @match expr begin
            ApplyHigher(f, []) => f
            AbstractHigher([], value) => value
            AbstractHigher(vars1, AbstractHigher(vars2, value)) => AbstractHigher(vcat(vars1, vars2), value)
            ApplyHigher(ApplyHigher(f, args1), args2) => ApplyHigher(f, vcat(args1, args2))
            ApplyHigher(AbstractHigher(vars, value), args) => begin
                n = min(length(vars), length(args))
                value = replace_expr(value, Dict(zip(vars[1:n], args[1:n])))
                ApplyHigher(AbstractHigher(vars[n+1:end], value), args[n+1:end])
            end
            _ => return map_expr(inline_higher, expr)
        end
    end
end

function inline(expr::Expr)::Expr
    expr = inline_let(expr)
    expr = inline_higher(expr)
end

# --- interpret ---

const Env{T} = Dict{Var, T}
function interpret(env::Env, expr::Expr)::ASet
    _interpret(env, expr)
end

function _interpret(env::Env{T}, expr::Var)::T where T
    env[expr]
end

function _interpret(env::Env, expr::Apply)::ASet
    f = interpret(env, expr.f)
    for arg in map((arg) -> interpret(env, arg), expr.args)
        result = ISet()
        for n in map(length, arg)
            for row in f
                if (length(row) >= n) && (row[1:n] in arg)
                    push!(result, row[n+1:end])
                end
            end
        end
        f = result
    end
    f
end

function _interpret(env::Env, expr::Abstract, var_ix::Int64)::ASet
    if var_ix > length(expr.vars)
        interpret(env, expr.value)
    else
        var = expr.vars[var_ix]
        result = ISet()
        for var_row in env[Var(:everything)]
            env[var] = ISet([var_row])
            for value_row in _interpret(env, expr, var_ix+1)
                push!(result, (var_row..., value_row...))
            end
        end
        result
    end
end

function _interpret(env::Env, expr::Abstract)::ASet
    _interpret(env, expr, 1)
end

function _interpret(env::Env, expr::Let)::ASet
    env[expr.var] = interpret(env, expr.value)
    interpret(env, expr.body)
end

function _interpret(env::Env, expr::Native)
    compile_error("Tried to interpret Native outside Apply: $expr")
end

# --- interpret values ---

function _interpret(env::Env{ASet}, expr::Constant) ::ASet
    expr.value
end

struct IndexedFunc
    f::Function
end

Base.getindex(func::IndexedFunc, key...) = func.f(key...)

function _interpret(env::Env{ASet}, expr::Primitive) ::ASet
    @match (expr.f, expr.args) begin
        (:reduce, [raw_op, raw_init, raw_values]) => begin
            op = if raw_op isa ConjunctiveQuery
                # TODO this is such a mess - need to at least reorder variables - not correct to assume that input vars get ordered first
                @assert raw_op.query_vars[1:2] == raw_op.yield_vars[1:2]
                (var_a, var_b) = raw_op.query_vars[1:2]
                raw_op = ConjunctiveQuery(setdiff(raw_op.yield_vars, [var_a, var_b]), raw_op.query_vars[3:end], raw_op.query_bounds[3:end], raw_op.clauses)
                operation(a,b) = begin
                    env[var_a] = SSet([(a,)])
                    env[var_b] = SSet([(b[end],)])
                    result = interpret(env, raw_op)
                    @assert length(result) == 1
                    @assert length(first(result)) == 1
                    first(first(result))
                end
                IndexedFunc(operation)
            elseif raw_op isa Native
                IndexedFunc(raw_op.f)
            else
                raw_op = interpret(env, raw_op)
                Dict(((a,b) => c for (a,b,c) in raw_op))
            end
            raw_init = interpret(env, raw_init)
            @assert length(raw_init) == 1
            @assert length(first(raw_init)) == 1
            init = first(raw_init)[1]
            raw_values = interpret(env, raw_values)
            value = reduce((a,b) -> op[a,b[end]], init, raw_values)
            SSet([(value,)])
        end
        _ => begin
            args = [interpret(env, arg) for arg in expr.args]
            @match (expr.f, args) begin
                (:|, [a, b]) => union(a,b)
                (:&, [a, b]) => intersect(a,b)
                (:!, [arg]) => bool_to_set(!set_to_bool(arg))
                (:(=>), [a, b]) => bool_to_set((!set_to_bool(a) || set_to_bool(b)))
                (:(==), [a, b]) => bool_to_set(a == b)
                (:iff, [cond, true_branch, false_branch]) => set_to_bool(cond) ? true_branch : false_branch
                (:exists, [arg]) => bool_to_set(arg != false_set)
                (:forall, [arg]) => bool_to_set(arg == env[Var(:everything)])
                (:tuple, args) => reduce(true_set, args) do a, b
                    ISet(((a_row..., b_row...) for a_row in a for b_row in b))
                end
                (:compose, [a, b]) => ISet(((a_row[1:end-1]..., b_row[2:end]...) for a_row in a for b_row in b if (length(a_row) > 0 && length(b_row) > 0) && a_row[end] == b_row[1]))
                _ => error("Unknown primitive: $expr")
            end
        end
    end
end


# TODO these are kinda hacky - should probably depend on native.out_types
return!(result::ASet, returned::Union{Vector, ASet}) = foreach(returned -> return!(result, returned), returned)
return!(result::ASet, returned::Tuple) = push!(result, returned)
return!(result::ASet, returned::Bool) = returned && push!(result, ())
return!(result::ASet, returned::Void) = nothing
return!(result::ASet, returned) = push!(result, (returned,))

function _interpret(env::Env{ASet}, expr::Apply) ::ASet
    if expr.f isa Native
        f = expr.f
        result = ISet()
        for row in interpret(env, Primitive(:tuple, expr.args))
            if length(f.in_types) == length(row)
                if all(vt -> vt[1] isa vt[2], zip(row, f.in_types))
                    returned = Base.invokelatest(f.f, row...)
                    return!(result, returned)
                end
            elseif length(f.in_types) + length(f.out_types) == length(row)
                tmp = ISet()
                in_row = row[1:length(f.in_types)]
                out_row = row[length(f.in_types)+1:end]
                if all(vt -> vt[1] isa vt[2], zip(in_row, f.in_types))
                    returned = Base.invokelatest(f.f, in_row...)
                    # TODO we only need to tmp because we don't know how to iter over returned without return!
                    return!(tmp, returned)
                end
                if out_row in tmp
                    push!(result, ())
                end
            else
                # TODO technically these other cases have valid semantics, but I don't care to execute them just yet
                compile_error("Cannot apply $f to $row")
            end
        end
        result
    else
        # fallback to default
        invoke(_interpret, Tuple{Env, Apply}, env, expr)
    end
end

# --- interpret types ---

const RowType = NTuple{N, Type} where N
const TSetType = TSet{RowType}

row_type(row::Tuple) = map(typeof, row)
set_type(set::TSet) = map(row_type, set)
env_types(env::Env{ASet}) = Env{TSetType}(name => set_type(set) for (name, set) in env)

const bool_type = TSetType([()])
const false_type = TSetType([])

function _interpret(env::Env{TSetType}, expr::Constant)::TSetType
    set_type(expr.value)
end

function _interpret(env::Env{TSetType}, expr::Primitive)::TSetType
    arg_types = [interpret(env, arg) for arg in expr.args]
    @match (expr.f, arg_types) begin
        (:|, [a, b]) => union(a, b)
        (:&, [a, b]) => intersect(a, b)
        (:!, [arg]) => bool_type # because we don't have a true_type
        (:(=>), [a, b]) => bool_type
        (:(==), [a, b]) => intersect(a, b) == false_type ? false_type : bool_type
        (:iff, [cond_type, then_type, else_type]) => begin
            # TODO if we had lower bounds on cond we could also do then_type when it's definitely true
            if cond_type == false_type
                else_type
            else
                union(then_type, else_type)
            end
        end
        (:reduce, [raw_op, raw_init, values]) => map(row_type -> (row_type[3],), raw_op)
        (:exists, [arg]) => arg == false_type ? false_type : bool_type
        (:forall, [arg]) => bool_type
        (:tuple, args) => reduce(true_set, args) do a, b
            TSet(((a_row..., b_row...) for a_row in a for b_row in b))
        end
        (:compose, [a, b]) => begin
            result = TSet()
            for a_row in a
                for b_row in b
                    if length(a_row) > 0 && length(b_row) > 0 && (a_row[end] == b_row[1])
                        push!(result, (a_row[1:end-1]..., b_row[2:end]...))
                    end
                end
            end
            result
        end
        _ => error("Unknown primitive: $expr")
    end
end

function _interpret(env::Env{TSetType}, expr::Native) ::TSetType
    TSetType([(expr.in_types..., expr.out_types...)])
end

# TODO totally gross global, pass a context instead
const expr_types = Dict{Expr, TSetType}()
function interpret(env::Env{TSetType}, expr::Expr)
    expr_type = _interpret(env, expr)
    # TODO really we want to union types of vars in their abstract, much more than the type of the expression
    union!(get!(expr_types, expr, TSetType()), expr_type)
    expr_type
end
function infer_types(env::Env{ASet}, types::TSet{Type}, expr::Expr)::Dict{Expr, TSetType}
    empty!(expr_types)
    interpret(push!(env_types(env), Var(:everything)=>TSet(((typ,) for typ in types))), expr)
    copy(expr_types)
end

# --- inference ---

function infer_arity(env::Env{NSet}, expr::Expr)::Dict{Expr, NSet}
    arities = Dict{Expr, NSet}()
    infer(expr::Expr)::NSet = begin
        arity = @match expr begin
            Constant(value) => NSet(length(row) for row in value)
            Var(_, 0) => env[expr]
            Var(_, _) => NSet(1)
            Apply(f, args) => begin
                result = NSet()
                for arity_f in infer(f)
                    for arity_args in infer(Primitive(:tuple, args))
                        if arity_f - arity_args >= 0
                            push!(result, arity_f - arity_args)
                        end
                    end
                end
                result
            end
            Primitive(f, args) => begin
                @match (f, map(infer, args)) begin
                    (:|, [a, b]) => union(a, b)
                    (:&, [a, b]) => intersect(a, b)
                    (:!, _) => NSet(0)
                    (:(=>), _) => NSet(0)
                    (:(==), _) => NSet(0)
                    (:iff, [c, t, f]) => union(t, f)
                    (:reduce, _) => NSet(1)
                    (:exists, _) => NSet(0)
                    (:forall, _) => NSet(0)
                    (:tuple, args) => reduce(NSet(0), args) do a, b
                        result = NSet()
                        for arity_a in a
                            for arity_b in b
                                push!(result, arity_a + arity_b)
                            end
                        end
                        result
                    end
                    (:compose, [a, b]) => begin
                        result = NSet()
                        for arity_a in a
                            for arity_b in b
                                if arity_a + arity_b - 2 >= 0
                                    push!(result, arity_a + arity_b - 2)
                                end
                            end
                        end
                        result
                    end
                end
            end
            Native(f, in_types, out_types) => NSet(length(in_types) + length(out_types))
            Abstract(vars, value) => NSet((length(vars) + av for av in infer(value)))
        end
        arities[expr] = arity
        arity
    end
    infer(expr)
    arities
end

# --- lower ---

const Arity = Union{Int64, Void} # false has arity nothing

function arity(arities::NSet)::Arity
    @match length(arities) begin
        0 => nothing
        1 => first(arities)
        _ => compile_error("Ill-typed: $arities")
    end
end

function replace_expr(expr::Expr, replacements::Dict)::Expr
    new_expr = haskey(replacements, expr) ? replacements[expr] : expr
    map_expr(expr -> replace_expr(expr, replacements), new_expr)
end

function desugar(arity::Dict{Expr, Arity}, last_id::Ref{Int64}, expr::Expr)::Expr
    desugar(expr) = @match expr begin
        Primitive(:tuple, args) => begin
            vars = Var[]
            body = reduce(Constant(true_set), args) do body, arg
                arg_vars = [Var(Symbol("_$(last_id[] += 1)"), 1) for _ in 1:coalesce(arity[arg], 0)]
                append!(vars, arg_vars)
                Primitive(:&, [body, Apply(arg, arg_vars)])
            end
            desugar(Abstract(vars, body))
        end
        Primitive(:compose, [a, b]) => begin
            coalesce(arity[a], 0) == 0 && return Constant(false_set)
            coalesce(arity[b], 0) == 0 && return Constant(false_set)
            a_vars = [Var(Symbol("_$(last_id[] += 1)"), 1) for _ in 1:coalesce(arity[a], 0)]
            b_vars = [Var(Symbol("_$(last_id[] += 1)"), 1) for _ in 1:coalesce(arity[b], 0)]
            var = b_vars[1] = a_vars[end]
            vars = vcat(a_vars[1:end-1], b_vars[2:end])
            desugar(Abstract(vars, Primitive(:exists, [Abstract([var], Primitive(:&, [Apply(a, a_vars), Apply(b, b_vars)]))])))
        end
        _ => map_expr(desugar, expr)
    end
    desugar(expr)
end

function is_scalar(expr::Expr)
    @match expr begin
        Constant(value) => (length(value) == 1) && (length(first(value)) == 1)
        Var(_, scope) => scope > 0
        _ => false
    end
end

# rewrite until every Apply has var/native f and boolean type and all args are bound vars or _
function simple_apply(arity::Dict{Expr, Arity}, last_id::Ref{Int64}, expr::Expr)::Expr
    make_slots(n) = begin
        slots = [Var(Symbol("_$(last_id[] += 1)"), 1) for _ in 1:n]
        for slot in slots
            arity[slot] = 1
        end
        slots
    end
    apply(expr::Expr) = @match arity[expr] begin
        nothing => Constant(false_set) # if it's provable false, why bother keeping it around?
        n => begin
            slots = make_slots(n)
            Abstract(slots, apply(expr, slots))
        end
    end
    apply(expr::Expr, slots::Vector{Var})::Expr = @match expr begin

        # things that don't make sense will be hard to lower
        _ where get(arity, expr, -1) == nothing => Constant(false_set)

        # false[slots...] => false
        Constant(value) where (value == false_set) => expr

        # true[] => true
        Constant(value) where (value == true_set) => begin
            @match [] = slots
            expr
        end

        Var(:everything, 0) => begin
            @match [slot] = slots
            Constant(true_set)
        end

        # scalar[slot] => slot==scalar
        _ where is_scalar(expr) => begin
            @match [slot] = slots
            Primitive(:(==), [slot, expr])
         end

        # f[slots...] => f(slots...)
        Constant(_) || Var(_,_) => Apply(expr, slots)

        # f(x,y)[slots...] => f(x)(y)[slots...]
        Apply(f, []) => apply(f, slots)
        Apply(f, args) where (length(args) > 1) => apply(reduce((f, arg) -> Apply(f, [arg]), f, args), slots)

        # f(x)[slots...] => f[x, slots...]
        Apply(f, [x && Var(name, scope)]) where (name == :everything || (scope != 0)) => apply(f, [x, slots...])

        # f(g)[slots...] => exists((new_slots...) -> f[new_slots..., slots...] & g[new_slots...])
        Apply(f, [g]) => begin
            new_slots = make_slots(arity[g])
            Primitive(:exists,
                      [Abstract(new_slots,
                                Primitive(:&, [
                                    apply(f, vcat(new_slots, slots)),
                                    apply(g, new_slots)
                                ]))])
        end

        # (a & b)[slots...] => a[slots...] & b[slots...]
        Primitive(:&, [a, b]) => Primitive(:&, [apply(a, slots), apply(b, slots)])

        # (a | b)[slots...] => a[slots...] & b[slots...]
        Primitive(:|, [a, b]) => Primitive(:|, [apply(a, slots), apply(b, slots)])

        # (!a)[] => !(a[...])
        Primitive(:!, [arg]) => Primitive(:!, [apply(arg)])

        # (a => b)[] => b[] | !a[]
        Primitive(:(=>), [a, b]) => begin
            @match [] = slots
            Primitive(:|, [Primitive(:!, [apply(a)]), apply(b)])
        end

        # leave scalar eq alone, otherwise lower isn't idempotent
        Primitive(:(==), [a, b]) where (is_scalar(a) && is_scalar(b)) => expr
        # (a == b)[] => (a[...] == b[...])
        Primitive(:(==), [a, b]) => Primitive(:(==), [apply(a), apply(b)])

        # (c ? t : f)[slots...] => (c[...] & t[slots...]) | (!c[...] & f[slots...])
        Primitive(:iff, [c, t, f]) => Primitive(:|,
                                                [Primitive(:&, [Primitive(:exists, [apply(c)]), apply(t, slots)]),
                                                 Primitive(:&, [Primitive(:!, [apply(c)]), apply(f, slots)])])

        # reduce(...)[slot] => slot==reduce(...)
        Primitive(:reduce, args) => begin
            @match [slot] = slots
            Primitive(:(==), [slot, Primitive(:reduce, map(apply, args))])
        end

        # E(arg)[] => E(arg[...])
        Primitive(:exists, [arg]) => Primitive(:exists, [apply(arg)])

        # something is wonky with negation - shouldn't need to do two reduction steps together
        # A(arg)[] => !(slot -> !arg[slot])
        Primitive(:forall, [arg]) => begin
            new_slots = make_slots(arity[arg])
            Primitive(:!, [Abstract(new_slots, Primitive(:!, [apply(arg, new_slots)]))])
        end

        # TODO this can't be interpreted until after bounding
        # n[slots...] => n(slots...)
        Native(_, _, _) => Apply(expr, slots)

        # ((vars...) -> value)[slots...] => value[vars... = slots...]
        Abstract(vars, value) => begin
            n = length(vars)
            @assert n <= length(slots)
            replace_expr(apply(value, slots[n+1:end]), Dict(zip(vars, slots[1:n])))
        end
    end
    apply(expr)
end

function negate(expr::Expr)::Expr
    @match expr begin
        Constant(value) where (value == false_set) => Constant(true_set)
        Constant(_) => Constant(false_set)
        # abstract vars can't be empty
        Var(_, scope) where scope > 0 => Constant(false_set)
        Primitive(:|, [a, b]) => Primitive(:&, [negate(a), negate(b)])
        Primitive(:&, [a, b]) => Primitive(:|, [negate(a), negate(b)])
        Primitive(:!, [arg]) => Primitive(:exists, [lower_negation(arg)])
        # TODO reduce?
        Primitive(:exists, [arg]) => negate(arg)
        Abstract([], value) => Abstract([], negate(value))
        _ => Primitive(:!, [lower_negation(expr)])
    end
end

function lower_negation(expr::Expr)::Expr
    @match expr begin
        Primitive(:!, [arg]) => negate(arg)
        _ => map_expr(lower_negation, expr)
    end
end

# DNF
# TODO creates duplicate vars - does that need to be fixed?
function raise_union(expr::Expr)::Expr
    raise(expr) = @match expr begin
        Abstract(vars, Primitive(:|, [a, b])) => Primitive(:|, [raise(Abstract(vars, a)), raise(Abstract(vars, b))])
        Primitive(:&, [Primitive(:|, [a1, a2]), b]) => Primitive(:|, [raise(Primitive(:&, [a1, b])), raise(Primitive(:&, [a2, b]))])
        Primitive(:&, [a, Primitive(:|, [b1, b2])]) => Primitive(:|, [raise(Primitive(:&, [a, b1])), raise(Primitive(:&, [a, b2]))])
        Primitive(:exists, [Primitive(:|, [a, b])]) => Primitive(:|, [raise(Primitive(:exists, [a])), raise(Primitive(:exists, [b]))])
        _ => expr
    end
    raise(map_expr(raise_union, expr))
end

function lower(env::Env{ASet}, types::TSet{Type}, expr::Expr)::Expr
    last_id = Ref(0)
    env_arities = Env{NSet}((name => NSet(length(row) for row in set) for (name, set) in env))
    env_arities[Var(:everything)] = NSet(1)

    arities = infer_arity(env_arities, expr)
    simple_arities = Dict{Expr, Arity}((name => arity(arities) for (name, arities) in arities))
    expr = desugar(simple_arities, last_id, expr)

    # TODO gross that we have to do inference twice
    arities = infer_arity(env_arities, expr)
    simple_arities = Dict{Expr, Arity}((name => arity(arities) for (name, arities) in arities))
    expr = simple_apply(simple_arities, last_id, expr)

    expr = lower_negation(expr)
    expr = raise_union(expr)

    expr
end

# --- bound ---

# equivalent to (yield_vars) -> E((query_vars/yield_vars) -> &(clauses...))
# easier to manipulate all in one place
struct ConjunctiveQuery <: Expr
    yield_vars::Vector{Var}
    query_vars::Vector{Var}
    query_bounds::Vector{Expr}
    clauses::Vector{Expr}
end

struct Permute <: Expr
    arg::Expr
    columns::Vector{Int64}
    dupe_columns::Vector{Pair{Int64, Int64}}
end

function unparse(expr::Union{Permute, ConjunctiveQuery})
    @match (expr, map_expr(unparse, tuple, expr)) begin
        (_::Permute, (arg, columns, dupe_columns)) => :($arg[$(columns...), $((:($a=$b) for (a,b) in dupe_columns)...)])
        (_::ConjunctiveQuery, (yield_vars, query_vars, query_bounds, clauses)) => begin
            body = if isempty(clauses)
                :(return ($(yield_vars...),))
            else
                :(if &($(clauses...)); return ($(yield_vars...),); end)
            end
            for (var, bound) in zip(reverse(query_vars), reverse(query_bounds))
                body = :(for $var in $bound; $body; end)
            end
            body
        end
    end
end

function _interpret(env::Env, expr::Permute)::ASet
    ISet((row[expr.columns] for row in interpret(env, expr.arg)
         if all((d) -> row[d[1]] == row[d[2]], expr.dupe_columns)))
end

function _interpret(env::Env, expr::ConjunctiveQuery, var_ix::Int64)::ASet
    if var_ix > length(expr.query_vars)
        if isempty(expr.clauses)
            SSet([()])
        else
            intersect(map(expr -> interpret(env, expr), expr.clauses)...)
        end
    else
        var = expr.query_vars[var_ix]
        bound = expr.query_bounds[var_ix]
        result = ISet()
        for var_row in interpret(env, bound)
            env[var] = SSet([var_row])
            for value_row in _interpret(env, expr, var_ix+1)
                push!(result, (var_row..., value_row...))
            end
        end
        result
    end
end

function _interpret(env::Env, expr::ConjunctiveQuery)::ASet
    yield_ixes = map(var -> findfirst(isequal(var), expr.query_vars), expr.yield_vars)
    ISet((row[yield_ixes] for row in _interpret(env, expr, 1)))
end

function permute(bound_vars::Vector{Var}, expr::Apply)::Expr
    apply_args = Var[]
    columns = Int64[]
    dupe_columns = Pair{Int64,Int64}[]
    permute_column!(bound_var) = begin
        bound_columns = findall(isequal(bound_var), expr.args)
        if !isempty(bound_columns)
            push!(apply_args, bound_var)
            push!(columns, bound_columns[1])
            for column in bound_columns[2:end]
                push!(dupe_columns, column => bound_columns[1])
            end
        end
    end
    foreach(permute_column!, bound_vars)
    Apply(Permute(expr.f, columns, dupe_columns), apply_args)
end

function permute(bound_vars::Vector{Var}, var::Var, expr::Apply)::Expr
    apply_args = Var[]
    columns = Int64[]
    dupe_columns = Pair{Int64,Int64}[]
    permute_column!(bound_var) = begin
        bound_columns = findall(isequal(bound_var), expr.args)
        if !isempty(bound_columns)
            push!(apply_args, bound_var)
            push!(columns, bound_columns[1])
            for column in bound_columns[2:end]
                push!(dupe_columns, column => bound_columns[1])
            end
        end
    end
    foreach(permute_column!, bound_vars)
    permute_column!(var)
    pop!(apply_args) # don't actually want to apply var
    Apply(Permute(expr.f, columns, dupe_columns), apply_args)
end

function is_scalar_bound(bound_vars::Vector{Var}, expr::Expr)
    @match expr begin
        Constant(value) => (length(value) == 1) && (length(first(value)) == 1)
        Var(_,_) where expr in bound_vars => true
        # TODO only true if reduce is over bound_vars
        Primitive(:reduce, _) => true
        _ => false
    end
end

# TODO this has a lot of overlap with bound_clauses - could probably combine them
function order_vars(bound_vars::Vector{Var}, vars::Vector{Var}, clauses::Vector{Expr})
    supported = Set(bound_vars)
    remaining = copy(vars)
    ordered = Var[]
    while !isempty(remaining)
        for clause in clauses
            @match clause begin
                Apply(f::Var, args) => begin
                    for arg in args
                        if arg isa Var
                            push!(supported, arg)
                        end
                    end
                end
                Apply(f::Native, args) => begin
                    if all(in(ordered), args[1:length(f.in_types)])
                        for arg in args
                            if arg isa Var
                                push!(supported, arg)
                            end
                        end
                    end
                end
                Primitive(:(==), [a, b]) => begin
                    if (a isa Var) && is_scalar_bound(ordered, b)
                        push!(supported, a)
                    elseif (b isa Var) && is_scalar_bound(ordered, a)
                        push!(supported, b)
                    end
                end
                Primitive(:!, [arg]) => nothing
            end
        end
        ix = findfirst(in(supported), remaining)
        if ix == 0 || ix == nothing
            # TODO can't bail here because some tests are unsupported
            warn("Cannot support $remaining with $clauses")
            append!(ordered, remaining)
            break
        end
        push!(ordered, remaining[ix])
        deleteat!(remaining, ix)
    end
    ordered
end

function conjunctive_query(expr::Abstract)::Expr
    used_vars = Var[]
    exists_vars = Var[]
    clauses = Expr[]
    is_false = false
    gather(expr) = @match expr begin
        Constant(value) where (value == false_set) => (is_false = true)
        Constant(value) where (value == true_set) => nothing
        Apply(f, vars) => begin
            push!(clauses, expr)
            append!(used_vars, filter(v -> v isa Var, vars))
        end
        Primitive(:(==), [a, b]) => begin
            push!(clauses, expr)
            a isa Var && push!(used_vars, a)
            b isa Var && push!(used_vars, b)
        end
        Primitive(:!, [arg]) => push!(clauses, expr)
        Primitive(:&, [a, b]) => begin
            gather(a)
            gather(b)
        end
        Primitive(:exists, [Abstract(vars, value)]) => begin
            append!(exists_vars, vars)
            gather(value)
        end
        # TODO this is daft
        Primitive(:exists, [value]) => gather(value)
        # TODO this is daft
        Abstract([], value) => gather(value)
    end
    gather(expr.value)
    is_false && return Constant(false_set)

    # heuristic - solve variables in the order they are mentioned
    var_order = unique(vcat(used_vars, expr.vars, exists_vars))
    query_vars = intersect(var_order, vcat(expr.vars, exists_vars))

    ConjunctiveQuery(copy(expr.vars), query_vars, Expr[], clauses)
end

function bound_clauses(bound_vars::Vector{Var}, var::Var, clauses::Vector{Expr})
    remaining = Expr[]
    bounds = Expr[]
    for clause in clauses
        @match clause begin
            Apply(f::Var, args) => begin
                if var in args
                    push!(bounds, permute(bound_vars, var, clause))
                end
                if !issubset(args, union(bound_vars, [var]))
                    push!(remaining, clause)
                end
            end
            Apply(f::Native, args) => begin
                if all(in(bound_vars), args[1:length(f.in_types)]) &&
                    (var in args[length(f.in_types)+1:end])
                    inner_apply = Apply(f, args[1:length(f.in_types)])
                    outer_apply = Apply(inner_apply, args[length(f.in_types)+1:end])
                    push!(bounds, permute(bound_vars, var, outer_apply))
                end
                if !(issubset(args, union(bound_vars, [var])) &&
                     (var in args[length(f.in_types)+1:end]))
                    push!(remaining, clause)
                end
            end
            Primitive(:(==), [a, b]) => begin
                if (a == var) && is_scalar_bound(bound_vars, b)
                    push!(bounds, b)
                elseif (b == var) && is_scalar_bound(bound_vars, a)
                    push!(bounds, a)
                else
                    push!(remaining, clause)
                end
            end
            Primitive(:!, [arg]) => begin
                push!(remaining, clause)
            end
        end
    end
    bounds = map(expr -> bound_abstract(bound_vars, expr), bounds)
    (bounds, remaining)
end

function bound_abstract(bound_vars::Vector{Var}, expr::Expr)::Expr
    @match expr begin
        _::Abstract => begin
            query = conjunctive_query(expr)
            query == Constant(false_set) && return query

            ordered_vars = order_vars(bound_vars, query.query_vars, query.clauses)

            bounds = Expr[]
            clauses = query.clauses
            for i in 1:length(ordered_vars)
                vars = ordered_vars[1:i-1]
                var = ordered_vars[i]
                (bound, clauses) = bound_clauses(vcat(bound_vars, vars), var, clauses)
                if isempty(bound)
                    push!(bounds, Var(:everything))
                else
                    push!(bounds, reduce((a,b) -> Primitive(:&, [a,b]), bound))
                end
            end
            clause_bound_vars = vcat(bound_vars, query.query_vars)
            clauses = map(clauses) do clause
                @match clause begin
                    Apply(f::Var, args) where issubset(args, union(clause_bound_vars, [Var(:everything)])) => permute(clause_bound_vars, clause)
                    Apply(f::Native, args) where issubset(args, union(clause_bound_vars, [Var(:everything)])) => begin
                        inner_apply = Apply(f, args[1:length(f.in_types)])
                        outer_apply = Apply(inner_apply, args[length(f.in_types)+1:end])
                        permute(bound_vars, outer_apply)
                    end
                    _ => clause
                end
            end
            clauses = map(expr -> bound_abstract(clause_bound_vars, expr), clauses)
            ConjunctiveQuery(query.yield_vars, ordered_vars, bounds, clauses)
        end
        _ => map_expr(expr -> bound_abstract(bound_vars, expr), expr)
    end
end
bound_abstract(expr::Expr) = bound_abstract(Var[], expr)

# --- indexing ---
# TODO this is a temporary hack until we can use more sensible data structures

struct Lookup <: Expr
    index::Dict{Tuple, ASet}
    args::Vector{Var}
end

function _interpret(env::Env, expr::Lookup)::ASet
    row = tuple((first(first(env[arg])) for arg in expr.args)...)
    get(expr.index, row, false_set)
end

function build_indexes(env::Env, expr::Expr)::Expr
    @match map_expr(expr -> build_indexes(env, expr), expr) begin
        Apply(f && Permute(Var(_, 0), columns, _), args) where (length(columns) == length(args)+1) => begin
            index = Dict{Tuple, ASet}()
            for row in interpret(env, f)
                push!(get!(() -> ISet(), index, row[1:end-1]), row[end:end])
            end
            Lookup(index, args)
        end
        other => other
    end
end

unparse(expr::Lookup) = :({Lookup(Dict($(first(expr.index))...), $(expr.args))})

# --- interface ---

@enum Pass PARSE INLINE LOWER BOUND INTERPRET
Base.:-(a::Pass, b::Pass) = Int64(a) - Int64(b)
Base.:+(a::Pass, b::Int64) = Pass(Int64(a) + b)
Base.:-(a::Pass, b::Int64) = Pass(Int64(a) - b)

global_env = Env{ASet}()
global_lib = Env{Expr}()

function imp(expr; globals=nothing, env=global_env, lib=global_lib, types=nothing, everything=nothing, passes=instances(Pass))
    if env == nothing
        if globals != nothing
            env = Env{ASet}(Var(name) => set for (name, set) in globals)
        else
            env = Env{ASet}()
        end
    end
    if everything != nothing
        env[Var(:everything)] = everything
    end
    if types == nothing
        types = TSet{Type}()
        for (_, set) in env
            for row in set
                for val in row
                    push!(types, typeof(val))
                end
            end
        end
    end
    if PARSE in passes
        expr = parse(expr)
        # TODO this is a hack
        if lib != nothing
            expr = replace_expr(expr, lib)
        end
        scope_env = push!(copy(env), Var(:everything) => ESet())
        expr = separate_scopes(Scope(scope_env), expr)
    end
    if INLINE in passes
        expr = inline(expr)
    end
    if LOWER in passes
        expr = lower(env, types, expr)
    end
    if BOUND in passes
        expr = bound_abstract(expr)
    end
    if INTERPRET in passes
        expr = build_indexes(env, expr)
        expr = interpret(env, expr)
    end
    expr
end

macro imp(expr)
    if @capture(expr, name_Symbol = value_)
        :(global_env[Var($(QuoteNode(name)))] = @imp $(value))
    else
        :(imp($(QuoteNode(expr)), env=global_env, lib=global_lib))
    end
end

function show_imp(set::ASet)
    for row in sort(collect(set))
        println(row)
    end
    set
end

macro imp!(expr)
    quote
        show_imp(@imp $(expr))
        nothing
    end
end

macro lib(expr)
    @assert @capture(expr, name_Symbol = value_)
    :(global_lib[Var($(QuoteNode(name)))] = imp($(QuoteNode(value)), env=global_env, lib=global_lib, passes=PARSE:INLINE))
end

export imp, @imp, @imp!, @lib

end
