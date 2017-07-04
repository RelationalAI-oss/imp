module UI

# Pkg.add("Blink")
# AtomShell.install()

using Data
using Query
using Flows
using Blink
using Match
using Hiccup

# --- ast ---

immutable StringExpr
  values::Vector{Union{String, Symbol}}
end

typealias Value Union{StringExpr, String, Symbol} 

abstract Node

immutable TextNode <: Node
  text::Value
end

immutable AttributeNode <: Node
  key::Value
  val::Value
end

immutable FixedNode <: Node
  tag::Value
  children::Vector{Node}
end

immutable QueryNode <: Node
  table::Symbol
  vars::Vector{Symbol}
  children::Vector{Node}
end

# --- parsing ---

function preprocess(expr)
  @match expr begin
    Expr(:vect || :vcat || :hcat, args, _) => map(preprocess, args)
    Expr(head, args, _) => Expr(head, map(preprocess, args)...)
    args::Vector => args
    value::Union{Symbol, String, Number} => value
    _ => error("What are this? $expr")
  end
end

function parse_value(expr)
  @match expr begin
    _::String => expr
    _::Symbol => expr
    Expr(:string, args, _) => StringExpr(map(parse_value, args))
    _ => error("What are this? $expr")
  end
end

function is_line_number(expr)
  @match expr begin
    Expr(:line, _, _) => true
    _ => false
  end
end

function parse_nodes(exprs)
  map(parse_node, exprs)
end

function parse_node(expr) 
  @match expr begin
    Expr(:call, [table::Symbol, children_expr, vars...], _) => begin
      @match children_expr begin
        Expr(:->, [Expr(:tuple, [], _), Expr(:block, children_exprs, _)], _) => begin
          children = parse_nodes(filter((e) -> !is_line_number(e), children_exprs))
          QueryNode(table, vars, children)
        end
        _ => error("What are this? $children_expr")
      end
    end
    [tag, args...] => FixedNode(parse_value(tag), parse_nodes(args))
    Expr(:(=), [key, val], _) => AttributeNode(parse_value(key), parse_value(val))
    text => TextNode(parse_value(text))
  end
end

function parse_template(expr)
  @match expr begin
    Expr(:block, [Expr(:line, _, _), child], _) => parse_node(preprocess(child))
    _ => parse_node(preprocess(expr))
  end
end

# --- compiling ---

type CompiledQueryNode
  id::Symbol
  parent_id::Symbol
  bound_vars::Vector{Symbol}
  query_node::QueryNode
  fragment::Vector{Union{Symbol, String}}
  compiled_fragment::Function
end

function generate_fragment(value::Union{String, Symbol}, fragment)
  push!(fragment, string(value))
end

function generate_fragment(expr::StringExpr, fragment)
  for value in expr.values
    push!(fragment, value)
  end
end

function generate_fragment(node::TextNode, fragment)
  generate_fragment(node.text, fragment)
end

function generate_fragment(node::AttributeNode, fragment)
  push!(fragment, " ")
  generate_fragment(node.key, fragment)
  push!(fragment, "=")
  generate_fragment(node.val, fragment)
end

function generate_fragment(node::FixedNode, fragment)
  push!(fragment, "<")
  generate_fragment(node.tag, fragment)
  for child in node.children
    if typeof(child) == AttributeNode
      generate_fragment(child, fragment)
    end
  end
  push!(fragment, ">")
  for child in node.children
    if typeof(child) != AttributeNode
      generate_fragment(child, fragment)
    end
  end
  push!(fragment, "</")
  generate_fragment(node.tag, fragment)
  push!(fragment, ">")
end

function generate_fragment(node::QueryNode, fragment)
  # TODO no html generated, but do we need to record the position or something? maybe put a dummy node in?
end

function concat_fragment(fragment)
  new_fragment = Union{Symbol, String}[]
  for value in fragment 
    if isa(value, String) && (length(new_fragment) > 0) && isa(new_fragment[end], String)
      new_fragment[end] = string(new_fragment[end], value)
    else
      push!(new_fragment, value)
    end
  end
  new_fragment
end

function compile_fragment(id::Symbol, node::QueryNode, bound_vars::Vector{Symbol})
  fragment = Union{Symbol, String}[]
  for child in node.children
    generate_fragment(child, fragment)
  end
  fragment = concat_fragment(fragment)
  name = Symbol("fragment_", id)
  fun = @eval function $(name)($(bound_vars...))
    string($(fragment...))
  end
  (fragment, fun)
end

function compile_query_nodes(parent_id, parent_bound_vars, node, compiled_query_nodes)
  if typeof(node) == QueryNode
    id = Symbol("query_node_", hash((parent_id, node)))
    bound_vars = copy(parent_bound_vars)
    for var in node.vars
      if !(var in parent_bound_vars)
        push!(bound_vars, var)
      end
    end
    (fragment, compiled_fragment) = compile_fragment(id, node, bound_vars)
    compiled_query_node = CompiledQueryNode(id, parent_id, bound_vars, node, fragment, compiled_fragment)
    push!(compiled_query_nodes, compiled_query_node)
    for child in node.children
      compile_query_nodes(id, bound_vars, child, compiled_query_nodes)
    end
  end
  if typeof(node) == FixedNode
    for child in node.children
      compile_query_nodes(parent_id, parent_bound_vars, child, compiled_query_nodes)
    end
  end
end

function compile_node(node)
  wrapper = QueryNode(:session, [:session], [node])
  compiled_query_nodes = []
  compile_query_nodes(:root, Symbol[], wrapper, compiled_query_nodes)
  @show compiled_query_nodes
end

function compile_server_tree(node, parent_id, parent_vars, flows)
  # do nothing by default
end

function compile_server_tree(node::FixedNode, parent_id, parent_vars, flows)
  id = Symbol("node_$(hash(node))")
  transient_flow = @eval @transient $id($([Any for _ in parent_vars]...)) => UInt64
  merge_flow = 
    if parent_id == nothing
      @eval @merge begin
        session(session)
        child_id = hash(session, $(hash(node)))
        return $id($(parent_vars...)) => child_id
      end
    else
      @eval @merge begin
        $parent_id($(parent_vars...)) => parent_id
        child_id = hash(parent_id, $(hash(node)))
        return $id($(parent_vars...)) => child_id
      end
    end
  push!(flows, transient_flow, merge_flow)
  for child in node.children
    compile_server_tree(child, id, parent_vars, flows)
  end
end

function compile_server_tree(node::QueryNode, parent_id, parent_vars, flows)
  id = Symbol("node_$(hash(node))")
  vars = unique(vcat(parent_vars, node.vars))
  transient_flow = @eval @transient $id($([Any for _ in vars]...)) => UInt64
  merge_flow = @eval @merge begin
    $parent_id($(parent_vars...)) => parent_id
    $(node.table)($(node.vars...))
    child_id = hash(tuple($(node.vars...), parent_id), $(hash(node)))
    return $id($(vars...)) => child_id
  end
  push!(flows, transient_flow, merge_flow)
  for child in node.children
    compile_server_tree(child, id, vars, flows)
  end
end

function collect_sort_key(node, parent_vars, key, keyed_children)
  # TODO handle attributes and text
end

function collect_sort_key(node::FixedNode, parent_vars, key, keyed_children)
  push!(keyed_children, (copy(key), parent_vars, node))
end

function collect_sort_key(node::QueryNode, parent_vars, key, keyed_children)
  vars = unique(vcat(parent_vars, node.vars))
  new_vars = vars[(1+length(parent_vars)):end]
  start_ix = length(key) + 1
  append!(key, new_vars) 
  end_ix = length(key)
  collect_sort_key(node.children, vars, key, keyed_children)
  for ix in start_ix:end_ix
    key[ix] = nothing
  end
end

function collect_sort_key(nodes::Vector{Node}, parent_vars, key, keyed_children)
  push!(key, 0)
  ix = length(key)
  for node in nodes
    if typeof(node) in [FixedNode, QueryNode] # TODO handle attributes and text
      key[ix] += 1
      collect_sort_key(node, parent_vars, key, keyed_children)
    end
  end
  key[ix] = 0
end

function collect_sort_key(node::FixedNode, parent_vars)
  key = Any[]
  keyed_children = Any[]
  collect_sort_key(node.children, parent_vars, key, keyed_children)
  # tidy up ragged ends of keys
  for (child_key, vars, child) in keyed_children
    @show child_key key[(length(child_key)+1):end]
    append!(child_key, key[(length(child_key)+1):end])
    @show child_key
  end
  keyed_children
end

# TODO this is defined in Julia 0.6 but can't currently upgrade because of https://github.com/kmsquire/Match.jl/issues/35
function Base.isless{T}(x::Nullable{T}, y::Nullable{T})
  !Base.isnull(x) && (Base.isnull(y) || (get(x) < get(y)))
end

function key_expr(elem) 
  @match elem begin
    _::Integer => elem
    _::Symbol => :(Nullable($elem))
    _::Void => :(Nullable())
    _ => error("What are this: $elem")
  end
end

function key_type(elem)
  @match elem begin
    _::Integer => Int64
    _::Symbol => Nullable{Any} # TODO figure out correct types somehow
    _::Void => Nullable{Any}
    _ => error("What are this: $elem")
  end
end

function compile_client_tree(node::FixedNode, parent_vars, flows, group_ids)
  keyed_children = collect_sort_key(node, parent_vars)
  group_id = Symbol("group_$(hash(node))")
  parent_node_id = Symbol("node_$(hash(node))")
  if !isempty(keyed_children)
    push!(group_ids, group_id)
  end
  for (key, child_vars, child) in keyed_children
    child_node_id = Symbol("node_$(hash(child))")
    key_exprs = map(key_expr, key)
    key_types = map(key_type, key)
    transient_flow = @eval @transient $group_id($(key_types...)) => (UInt64, UInt64, String)
    merge_flow = @eval @merge begin
      $parent_node_id($(parent_vars...)) => parent_id
      $child_node_id($(child_vars...)) => child_id
      return $group_id($(key_exprs...)) => (parent_id, child_id, $(string(child.tag)))
    end
    push!(flows, transient_flow, merge_flow)
  end
  for (_, child_vars, child) in keyed_children
    compile_client_tree(child, child_vars, flows, group_ids)
  end
end

function compile_template(node)
  flows = Flow[]
  group_ids = Symbol[]
  compile_server_tree(node, nothing, [:session], flows)
  compile_client_tree(node, [:session], flows, group_ids)
  @show flows
  (Sequence(flows), group_ids)
end

# --- interpreting ---
# dumb slow version just to get the logic right
# TODO not that

function interpret_value(value, bound_vars)
  if isa(value, StringExpr) 
    string((isa(v,Symbol) ? string(bound_vars[v]) : v for v in value.values)...)
  else
    string(value)
  end
end

function interpret_node(parent, node::TextNode, bound_vars, state)
  push!(parent.children, interpret_value(node.text, bound_vars))
end

function interpret_node(parent, node::AttributeNode, bound_vars, state)
  key = interpret_value(node.key, bound_vars)
  val = interpret_value(node.val, bound_vars)
  parent.attrs[key] = string(get(parent.attrs, key, ""), val) 
end

function interpret_node(parent, node::FixedNode, bound_vars, state)
  tag = Symbol(interpret_value(node.tag, bound_vars))
  child = Hiccup.Node(tag)
  push!(parent.children, child)
  for grandchild in node.children
    interpret_node(child, grandchild, bound_vars, state) 
  end
end

function interpret_node(parent, node::QueryNode, bound_vars, state)
  columns = state[node.table].columns
  @assert length(node.vars) == length(columns)
  for r in 1:length(columns[1])
    if all((!(var in keys(bound_vars)) || (bound_vars[var] == columns[c][r]) for (c, var) in enumerate(node.vars)))
      new_bound_vars = copy(bound_vars)
      for (c, var) in enumerate(node.vars)
        new_bound_vars[var] = columns[c][r]
      end
      for child in node.children
        interpret_node(parent, child, new_bound_vars, state)
      end
    end
  end
end

# --- plumbing ---

type View
  template::Any
  parsed::Node
  compiled::Flow
  group_names::Vector{Symbol}
  watchers::Set{Any}
end

function View() 
  View(
    nothing, 
    FixedNode("body", [TextNode("loading...")]), 
    Sequence(Flow[]), 
    Symbol[], 
    Set{Any}()
  )
end

function set_template!(view::View, template)
  view.template = template
  view.parsed = parse_template(template)
  (view.compiled, view.group_names) = compile_template(view.parsed)
  for watcher in view.watchers
    watcher()
  end
end

function Flows.watch(watcher, view::View)
  push!(view.watchers, watcher)
end

# TODO figure out how to handle changes in template
# TODO handle sessions
function render(window, view, world, session)
  for event in world.events
    js(window, Blink.JSString("$event = imp_event(\"$event\")"), callback=false) # these never return! :(
  end
  old_groups = Dict{Symbol, Union{Relation, Void}}(name => get(world.state, name, nothing) for name in view.group_names)
  Flows.init_flow(view.compiled, world)
  Flows.run_flow(view.compiled, world)
  new_groups = Dict{Symbol, Relation}(name => world.state[name] for name in view.group_names)
  for name in view.group_names
    if old_groups[name] == nothing
      old_groups[name] = empty(new_groups[name])
    end
  end
  deletes = Set{UInt64}()
  creates = Vector{Tuple{UInt64, Union{UInt64, Void}, UInt64, String}}()
  for name in view.group_names
    old_columns = old_groups[name].columns
    old_parent_ids = old_columns[end-2]
    old_child_ids = old_columns[end-1]
    new_columns = new_groups[name].columns
    new_parent_ids = new_columns[end-2]
    new_child_ids = new_columns[end-1]
    new_contents = new_columns[end-0]
    Data.foreach_diff(old_columns, new_columns, old_columns[1:end-3], new_columns[1:end-3],
      (o, i) -> begin
        if !(old_parent_ids[i] in deletes)
          push!(deletes, old_child_ids[i])
        end
      end,
      (n, i) -> begin
        if (i < length(new_parent_ids)) && (new_parent_ids[i] == new_parent_ids[i+1])
          sibling = new_child_ids[i+1]
        else 
          sibling = nothing
        end
        push!(creates, (new_parent_ids[i], sibling, new_child_ids[i], new_contents[i]))
      end,
      (o, n, oi, ni) -> ()
    )
  end
  @show deletes creates
  @js(window, render($deletes, $creates))
end

function watch_and_load(window, file)
  load!(window, file)
  @schedule begin
    (waits, _) = open(`inotifywait -me CLOSE_WRITE $file`)
    while true
      readline(waits)
      load!(window, file)
    end
  end
end

pre = Sequence([
  @stateful session(String)
])

function window(world, view)
  window = Window()
  session = string(now()) # TODO uuid
  opentools(window)
  load!(window, "src/diffhtml.js")
  load!(window, "src/Imp.js")
  @js_(window, session = $session)
  sleep(3) # :(
  handle(window, "event") do event
    refresh(world, Symbol(event["table"]), tuple(event["values"]...))
  end
  watch(world) do old_state, new_state
    render(window, view, world, session)
  end
  watch(view) do
    render(window, view, world, session)
  end
  refresh(world, :session, tuple(session))
  window
end

export View, set_template!, window, watch

end
