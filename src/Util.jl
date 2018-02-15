module Util

macro showtime(expr)
  quote
    result = @time $(esc(expr))
    println($(string("^ ", expr)))
    println()
    result
  end
end

macro splice(iterator, body)
  @assert iterator.head == :call
  @assert iterator.args[1] == :in
  Expr(:..., :(($(esc(body)) for $(esc(iterator.args[2])) in $(esc(iterator.args[3])))))
end

export @showtime, @splice

# TODO distinguish better between static and dynamic calls
# invoke with non-leaf types may be dynamic
# call with leaf types may be static (grab the method instance?)
# TODO print warnings nicely
# TODO stop warning about exceptions

function get_method_instance(f, typs)
  world = ccall(:jl_get_world_counter, UInt, ())
  tt = typs isa Type ? Tuple{typeof(f), typs.parameters...} : Tuple{typeof(f), typs...}
  results = Base._methods_by_ftype(tt, -1, world)
  @assert length(results) == 1 "get_method_instance returned multiple methods: $results"
  (_, _, meth) = results[1]
  # TODO not totally sure what jl_match_method is needed for - I think it's just extracting type parameters like `where {T}`
  (ti, env) = ccall(:jl_match_method, Any, (Any, Any), tt, meth.sig)::SimpleVector
  meth = Base.func_for_method_checked(meth, tt)
  linfo = ccall(:jl_specializations_get_linfo, Ref{Core.MethodInstance}, (Any, Any, Any, UInt), meth, tt, env, world)
end

function get_code_info(method_instance::Core.MethodInstance)
  world = ccall(:jl_get_world_counter, UInt, ())
  params = Core.Inference.InferenceParams(world)
  optimize = true # TODO optimize=false prevents inlining, which is useful, but also prevents static calls from being detected
  (_, code_info, return_typ) = Core.Inference.typeinf_code(method_instance, optimize, true, params)
  (code_info, return_typ)
end

# TODO in 0.7, should this be `isdispatchtuple`?
function is_safe_type(typ::Type)
  isleaftype(typ) && (typ != Core.Box)
end

"Does this expression never have a real type?"
function is_untypeable(expr::Expr)
  expr.head in [:(=), :line, :boundscheck, :gotoifnot, :return, :meta, :inbounds, :throw] || (expr.head == :call && expr.args[1] == :throw)
end

struct MethodResult 
  typ::Type
end

@enum WarningKind NotConcretelyTyped Boxed DynamicCall

const Location = Union{Expr, TypedSlot, MethodResult}

struct Warning
  kind::WarningKind
  location::Location
end

struct Warnings
  code_info::CodeInfo # needed for nice printing of Slot
  warnings::Vector{Warning}
end

function warn_type!(location::Location, typ::Type, warnings::Vector{Warning})
  if !isleaftype(typ)
    push!(warnings, Warning(NotConcretelyTyped, location))
  end
  
  if typ == Core.Box
    push!(warnings, Warning(Boxed, location))
  end
end

function warn!(result::MethodResult, warnings::Vector{Warning})
  warn_type!(result, result.typ, warnings)
end

function warn!(expr::Expr, warnings::Vector{Warning})
  # many Exprs always have type Any
  if !(expr.head in [:(=), :line, :boundscheck, :gotoifnot, :return, :meta, :inbounds]) && !(expr.head == :call && expr.args[1] == :throw) 
    warn_type!(expr, expr.typ, warnings)
    if expr.head == :call
      # TODO logic for checking dynamic calls is tricky
      # should check that type of arg[1] is known eg GlobalRef
      # if any((arg) -> !is_safe_type(arg.typ), expr.args[2:end])
      #   push!(warnings, Warning(DynamicCall, expr))
      # end
    end
  end
end

function warn!(slot::TypedSlot, warnings::Vector{Warning})
  warn_type!(slot, slot.typ, warnings)
end

function get_warnings(method_instance::Core.MethodInstance)
  code_info, return_typ = get_code_info(method_instance)
  warnings = Warning[]
  
  warn!(MethodResult(return_typ), warnings)
  
  slot_is_used = [false for _ in code_info.slotnames]
  function walk_expr(expr)
      if isa(expr, Slot)
        slot_is_used[expr.id] = true
      elseif isa(expr, Expr)
        warn!(expr, warnings)
        foreach(walk_expr, expr.args)
      end
  end
  foreach(walk_expr, code_info.code)
  
  for (slot, is_used) in enumerate(slot_is_used)
    if is_used
      typ = code_info.slottypes[slot]
      warn!(TypedSlot(slot, typ), warnings)
    end
  end
  
  Warnings(code_info, warnings)
end

function get_child_calls(method_instance::Core.MethodInstance)
  code_info, return_typ = get_code_info(method_instance)
  calls = Set{Core.MethodInstance}()
  
  function walk_expr(expr)
      if isa(expr, Core.MethodInstance)
        push!(calls, expr)
      elseif isa(expr, Expr)
        foreach(walk_expr, expr.args)
      end
  end
  foreach(walk_expr, code_info.code)
  
  calls
end

function call_graph(method_instance::Core.MethodInstance, max_calls=1000::Int64) ::Vector{Pair{Core.MethodInstance, Set{Core.MethodInstance}}}
  all = Dict{Core.MethodInstance, Set{Core.MethodInstance}}()
  ordered = Vector{Core.MethodInstance}()
  unexplored = Set{Core.MethodInstance}((method_instance,))
  for _ in 1:max_calls
    if isempty(unexplored)
      return [call => all[call] for call in ordered]
    end
    method_instance = pop!(unexplored)
    child_calls= get_child_calls(method_instance)
    all[method_instance] = child_calls
    push!(ordered, method_instance)
    for child_call in child_calls
      if !haskey(all, child_call)
        push!(unexplored, child_call)
      end
    end
  end
  error("call_graph reached $max_calls calls and gave up")
end

function pretty(method_instance::Core.MethodInstance)
  original = string(method_instance)
  shortened = replace(original, "MethodInstance for ", "")
  method = method_instance.def
  "$shortened in $(method.module) at $(method.file):$(method.line)"
end

function pretty(code_info::CodeInfo, warning::Warning)
  slotnames = Base.sourceinfo_slotnames(code_info)
  buffer = IOBuffer()
  io = IOContext(buffer, :TYPEEMPHASIZE => true, :SOURCEINFO => code_info, :SOURCE_SLOTNAMES => slotnames)
  Base.emphasize(io, string(warning.kind)); print(io, ": "); Base.show_unquoted(io, warning.location)
  String(buffer)
end

function analyze(f, typs, filter::Function)
  for (call, child_calls) in call_graph(get_method_instance(f, typs))
    if filter(call)
      println();
      print(pretty(call)); println();
      for child_call in child_calls
        print("  Calls: "); print(pretty(child_call)); println();
      end
      warnings = get_warnings(call)
      for warning in warnings.warnings
        print("  "); print(pretty(warnings.code_info, warning)); println();
      end
    end
  end
end

@eval begin
  macro warnings(ex0)
    Base.gen_call_with_extracted_types($(Expr(:quote, :warnings)), ex0)
  end
  macro analyze(filter, ex0)
    expr = Base.gen_call_with_extracted_types($(Expr(:quote, :analyze)), ex0)
    push!(expr.args, esc(filter))
    expr
  end
  macro analyze(ex0)
    :(@analyze((_) -> true, $(esc(ex0))))
  end
end

end
