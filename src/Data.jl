module Data

using Match
using Base.Test

@generated function cmp_in{T <: Tuple}(xs::T, ys::T, x_at::Int, y_at::Int)
  n = length(T.parameters)
  if n == 0
    :(return 0)
  else 
    quote
      $(Expr(:meta, :inline))
      @inbounds begin 
        $([:(result = cmp(xs[$c][x_at], ys[$c][y_at]); if result != 0; return result; end) for c in 1:(n-1)]...)
        return cmp(xs[$n][x_at], ys[$n][y_at])
      end
    end
  end
end

@generated function swap_in{T <: Tuple}(xs::T, i::Int, j::Int)
  n = length(T.parameters)
  quote
    $(Expr(:meta, :inline))
    @inbounds begin 
      $([quote 
        let tmp = xs[$c][i]
          xs[$c][i] = xs[$c][j]
          xs[$c][j] = tmp
        end	    
      end for c in 1:n]...)
    end
  end
end

@generated function push_in!{T <: Tuple}(result::T, cs::T, i::Int)
  n = length(T.parameters)
  quote
    $(Expr(:meta, :inline))
    @inbounds begin 
      $([:(push!(result[$c], cs[$c][i])) for c in 1:n]...)
    end
  end
end

# sorting cribbed from Base.Sort
# but unrolls `cmp` and `swap` to avoid heap allocation of rows
# and use random pivot selection because stdlib pivot caused 1000x sadness on real data

function insertion_sort!{T <: Tuple}(cs::T, lo::Int, hi::Int)
  @inbounds for i = lo+1:hi
    j = i
    while j > lo && (cmp_in(cs, cs, j, j-1) == -1)
      swap_in(cs, j, j-1)
      j -= 1
    end
  end
end

function partition!{T <: Tuple}(cs::T, lo::Int, hi::Int)
  @inbounds begin
    pivot = rand(lo:hi)
    swap_in(cs, pivot, lo)
    i, j = lo+1, hi
    while true
      while (i <= j) && (cmp_in(cs, cs, i, lo) == -1); i += 1; end;
      while (i <= j) && (cmp_in(cs, cs, lo, j) == -1); j -= 1; end;
      i >= j && break
      swap_in(cs, i, j)
      i += 1; j -= 1
    end
    swap_in(cs, lo, j)
    return j
  end
end

function quicksort!{T <: Tuple}(cs::T, lo::Int, hi::Int)
  @inbounds if hi-lo <= 0
    return
  elseif hi-lo <= 20 
    insertion_sort!(cs, lo, hi)
  else
    j = partition!(cs, lo, hi)
    quicksort!(cs, lo, j-1)
    quicksort!(cs, j+1, hi)
  end
end

function quicksort!{T <: Tuple}(cs::T)
  quicksort!(cs, 1, length(cs[1]))
end

# TODO should be typed {K,V} instead of {T}, but pain in the ass to change now
abstract type Relation{T <: Tuple} end # where T is a tuple of columns

mutable struct MemoryRelation{T <: Tuple} <: Relation{T}
  r_columns::T
  r_num_keys::Int
  r_indexes::Dict{Vector{Int},T}
end

@inline function get_rel_index!{T <: Tuple}(f::Function, rel::MemoryRelation{T}, key)
  get!(f, rel.r_indexes, key)
end

@inline function get_rel_columns{T <: Tuple}(rel::MemoryRelation{T})
  rel.r_columns
end

@inline function get_rel_column{T <: Tuple}(rel::MemoryRelation{T}, idx::Int)
  rel.r_columns[idx]
end

@inline function get_rel_num_keys{T <: Tuple}(rel::MemoryRelation{T})
  rel.r_num_keys
end

@inline function get_rel_index{T <: Tuple}(rel::MemoryRelation{T}, key)
  rel.r_indexes[key]
end

function replace!{T <: Tuple}(old::MemoryRelation{T}, new::MemoryRelation{T})
  old.r_columns = get_rel_columns(new)
  old.r_num_keys = get_rel_num_keys(new)
  old.r_indexes = copy(get_rel_index(new)) # shallow copy of Dict
  old
end

mutable struct CloudRelation{T <: Tuple} <: Relation{T}
  r_name::String # the relation name, which is mostly used for easier debugging and a reference point for finding the relation ID
  r_id::UInt64 # relation ID is the main key used to lookup a relation from the cloud
  r_is_loaded::Bool # a flag that determines whether the relation is already loaded or nor
  r_is_dirty::Bool # a flag that shows whether the relation contains data that is not still stored on the cloud if it's a persistent relation
  r_is_persistent::Bool # a flag that determines whether the relation should be persistet on the cloud
  r_memory_data::MemoryRelation{T} # when the data is loaded from the cloud, it is stored in this MemoryRelation
end

function load_rel!{T <: Tuple}(rel::CloudRelation{T}, force::Bool=false)
  if force || !rel.r_is_loaded
    #TODO actually load the relation via PagerWrap
    rel.r_is_loaded = true
  end
end

function store_rel!{T <: Tuple}(rel::CloudRelation{T}, force::Bool=false)
  if force || (rel.r_is_dirty && rel.r_is_persistent)
    #TODO actually store the relation via PagerWrap
    rel.r_is_dirty = false
  end
end

@inline function get_rel_index!{T <: Tuple}(f::Function, rel::CloudRelation{T}, key)
  get_rel_index!(f, rel.r_memory_data, key)
end

@inline function get_rel_columns{T <: Tuple}(rel::CloudRelation{T})
  get_rel_columns(rel.r_memory_data)
end

@inline function get_rel_column{T <: Tuple}(rel::CloudRelation{T}, idx::Int)
  get_rel_column(rel.r_memory_data, idx)
end

@inline function get_rel_num_keys{T <: Tuple}(rel::CloudRelation{T})
  get_rel_num_keys(rel.r_memory_data)
end

@inline function get_rel_index{T <: Tuple}(rel::CloudRelation{T}, key)
  get_rel_index(rel.r_memory_data, key)
end

function replace!{T <: Tuple}(old::CloudRelation{T}, new::CloudRelation{T})
  replace!(old.r_memory_data, new.r_memory_data)
end

function create_relation(columns::T, num_keys::Int=length(columns)-1, is_paged::Bool=false, is_persistent::Bool=false, rel_name::String="", rel_id::UInt64=typemin(UInt64)) where {T <: Tuple}
  if is_paged
    rel = CloudRelation(rel_name, rel_id, true, is_persistent, is_persistent, create_relation(columns, num_keys, false, is_persistent, rel_name, rel_id))
    @assert !is_persistent || (!isempty(rel_name) && rel_id > typemin(UInt64))
    store_rel(rel)
    rel
  else
    order = collect(1:length(columns))
    if is_unique_and_sorted(columns)
      MemoryRelation{T}(columns, num_keys, Dict{Vector{Int}, typeof(columns)}(order => columns))
    else
      quicksort!(columns)
      deduped::typeof(columns) = map((column) -> empty(column), columns)
      key = columns[1:num_keys]
      val = columns[num_keys+1:1]
      dedup_sorted!(columns, key, val, deduped)
      MemoryRelation{T}(deduped, num_keys, Dict{Vector{Int}, typeof(deduped)}(order => deduped))
    end
  end
end

function is_sorted{T}(columns::T)
  for i in 2:length(columns[1])
    if cmp_in(columns, columns, i, i-1) == -1
      return false
    end
  end
  return true
end

function is_unique_and_sorted{T}(columns::T)
  for i in 2:length(columns[1])
    if cmp_in(columns, columns, i, i-1) != 1
      return false
    end
  end
  return true
end

function index{T}(relation::Relation{T}, order::Vector{Int})
  get_rel_index!(relation, order) do
    columns = tuple(((ix in order) ? copy(column) : empty(column) for (ix, column) in enumerate(get_rel_columns(relation)))...)
    sortable_columns = tuple((columns[ix] for ix in order)...)
    if !is_sorted(sortable_columns)
      quicksort!(sortable_columns)
    end
    columns
  end::T
end

function dedup_sorted!{T}(columns::T, key, val, deduped::T)
  at = 1
  hi = length(columns[1])
  while at <= hi
    push_in!(deduped, columns, at)
    while (at += 1; (at <= hi) && cmp_in(key, key, at, at-1) == 0) # skip dupe keys
      @assert cmp_in(val, val, at, at-1) == 0 # no key collisions allowed
    end
  end
end

function parse_relation(expr)
  (head, tail) = @match expr begin
    Expr(:call, [:(=>), head, tail], _) => (head, tail)
    head => (head, :(()))
  end
  (name, keys) = @match head begin
    Expr(:call, [name, keys...], _) => (name, keys)
    Expr(:tuple, keys, _) => ((), keys)
    _ => error("Can't parse $expr as relation")
  end
  vals = @match tail begin 
    Expr(:tuple, vals, _) => vals
    _ => [tail]
  end
  (name, keys, vals)
end

function column_type{T}(_::Type{T})
  Vector{T}
end

# TODO not useful until stack allocation of structs is improved
# function column_type{T}(_::Type{Nullable{T}})
#   NullableVector{T}
# end

# examples:
# @relation (Int, Float64)
# @relation (Int,) => Int
# @relation height_at(Int) => Float64
# @relation married(String, String)
# @relation state() => (Int, Symbol)
macro relation(expr) 
  (name, keys, vals) = parse_relation(expr)
  typs = [keys..., vals...]
  order = collect(1:length(typs))
  body = quote 
    columns = tuple($([:(column_type($(esc(typ)))()) for typ in typs]...))
    create_relation(columns, $(length(keys)))
  end
  if name != ()
    :(const $(esc(name)) = $body)
  else
    body
  end
end

function foreach_diff{T <: Tuple, K <: Tuple}(old::T, new::T, old_key::K, new_key::K, old_only, new_only, old_and_new)
  @inbounds begin
    old_at = 1
    new_at = 1
    old_hi = length(old[1])
    new_hi = length(new[1])
    while old_at <= old_hi && new_at <= new_hi
      c = cmp_in(old_key, new_key, old_at, new_at)
      if c == 0
        old_and_new(old, new, old_at, new_at)
        old_at += 1
        new_at += 1
      elseif c == 1
        new_only(new, new_at)
        new_at += 1
      else 
        old_only(old, old_at)
        old_at += 1
      end
    end
    while old_at <= old_hi
      old_only(old, old_at)
      old_at += 1
    end
    while new_at <= new_hi
      new_only(new, new_at)
      new_at += 1
    end
  end
end

function diff{T}(old::Relation{T}, new::Relation{T})
  @assert get_rel_num_keys(old) == get_rel_num_keys(new)
  order = collect(1:length(get_rel_columns(old)))
  old_index = index(old, order)
  new_index = index(new, order)
  old_only_columns = tuple([empty(column) for column in get_rel_columns(old)]...)
  new_only_columns = tuple([empty(column) for column in get_rel_columns(new)]...)
  foreach_diff(old_index, new_index, old_index, new_index,
    (o, i) -> push_in!(old_only_columns, o, i),
    (n, i) -> push_in!(new_only_columns, n, i),
    (o, n, oi, ni) -> ())
  (old_only_columns, new_only_columns)
end

function diff_ixes{T}(old::Relation{T}, new::Relation{T})::Tuple{Vector{Int64}, Vector{Int64}}
  @assert get_rel_num_keys(old) == get_rel_num_keys(new)
  order = collect(1:length(get_rel_columns(old)))
  old_index = index(old, order)
  new_index = index(new, order)
  old_only_ixes = Vector{Int64}()
  new_only_ixes = Vector{Int64}()
  foreach_diff(old_index, new_index, old_index, new_index,
    (o, i) -> push!(old_only_ixes, i),
    (n, i) -> push!(new_only_ixes, i),
    (o, n, oi, ni) -> ())
  (old_only_ixes, new_only_ixes)
end

function Base.merge{T}(old::Relation{T}, new::Relation{T})
  if get_rel_num_keys(old) != get_rel_num_keys(new)
    error("Mismatch in num_keys - $(get_rel_num_keys(old)) vs $(get_rel_num_keys(new)) in merge($old, $new)")
  end
  if length(get_rel_column(old, 1)) == 0
    return new
  end
  if length(get_rel_column(new, 1)) == 0
    return old
  end
  order = collect(1:length(get_rel_columns(old)))
  old_index = old.get_rel_index(order)
  new_index = new.get_rel_index(order)
  result_columns::T = tuple((empty(column) for column in get_rel_columns(old))...)
  foreach_diff(old_index, new_index, old_index[1:get_rel_num_keys(old)], new_index[1:get_rel_num_keys(new)],
    (o, i) -> push_in!(result_columns, o, i),
    (n, i) -> push_in!(result_columns, n, i),
    (o, n, oi, ni) -> push_in!(result_columns, n, ni))
  result_indexes = Dict{Vector{Int}, Tuple}(order => result_columns)
  create_relation{T}(result_columns, get_rel_num_keys(old), result_indexes)
end

function Base.merge!{T}(old::Relation{T}, new::Relation{T})
  replace!(old, merge(old, new))
end

function Base.push!{T}(relation::Relation{T}, values)
  merge!(relation, create_relation(map((i) -> eltype(get_rel_column(relation, i))[values[i]], tuple(1:length(values)...)), get_rel_num_keys(relation)))
end

@inline function Base.length(relation::Relation)
  length(get_rel_columns(relation))
end

@inline function Base.getindex(relation::Relation, ix)
  get_rel_column(relation, ix)
end

function empty(coll)
  typeof(coll)()
end

function empty(relation::Relation)
  create_relation(map((c) -> empty(c), get_rel_columns(relation)), get_rel_num_keys(relation), empty(get_rel_index(relation)))
end

function Base.copy(relation::Relation)
  create_relation(get_rel_columns(relation), get_rel_num_keys(relation), copy(get_rel_index(relation)))
end

export Relation, create_relation, get_rel_index, get_rel_column, get_rel_columns, get_rel_num_keys, @relation, index, parse_relation, empty, diff

end
