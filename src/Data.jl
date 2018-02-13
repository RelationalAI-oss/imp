module Data

using Memento
using Match
using Base.Test
using CxxWrap
using PagerWrap

logger = Memento.config("debug"; fmt="[{level} | {name}]: {msg}")

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

@inline function get_rel_index!(f::Function, rel::MemoryRelation{T}, key) where {T <: Tuple}
  get!(f, rel.r_indexes, key)
end

@inline function get_rel_columns(rel::MemoryRelation{T}) where {T <: Tuple}
  rel.r_columns
end

@inline function get_rel_column(rel::MemoryRelation{T}, idx::Int) where {T <: Tuple}
  rel.r_columns[idx]
end

@inline function get_rel_num_keys(rel::MemoryRelation{T}) where {T <: Tuple}
  rel.r_num_keys
end

@inline function get_rel_index(rel::MemoryRelation{T}, key) where {T <: Tuple}
  rel.r_indexes[key]
end

function replace!(old::MemoryRelation{T}, new::MemoryRelation{T}) where {T <: Tuple}
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
  r_num_keys::Int # number of key columns
  r_memory_data::Union{MemoryRelation{T}, Void} # when the data is loaded from the cloud, it is stored in this MemoryRelation
  r_pager_client::PagerWrap.PagerClient # a reference to the pager client for loading/storing the relation
  r_cloud_bucket::String
end

function deserialize_cloud_relation(rel::CloudRelation{T}, relation_page) where {T <: Tuple}
  relation_page_content = get_raw_content(relation_page)
  relation_page_length = get_page_length(relation_page)
  relation_page_content_array = unsafe_wrap(Array{UInt8, 1}, relation_page_content, relation_page_length)
  debug(logger, "Loading relation_page_content_array::$(typeof(relation_page_content_array)) with len = $(length(relation_page_content_array)) for rel.r_name=$(rel.r_name) and rel.r_id=$(rel.r_id)")
  read_iob = IOBuffer(relation_page_content_array)
  r_columns = deserialize(read_iob)
  r_num_keys = deserialize(read_iob)
  r_indexes = deserialize(read_iob)
  # We might have done a compression on the schema types (e.g., Job queries)
  # and we might not have access to the compressed schema types in another run
  # That is why we need to do this check and apply the type conversion if necessary
  if(typeof(r_columns) != T)
    r_columns = convert(T, r_columns)
    r_indexes = convert(Dict{Vector{Int},T}, r_indexes)
  end
  create_relation(r_columns, r_num_keys, r_indexes)
end

function load_rel!(rel::CloudRelation{T}, force::Bool=false) where {T <: Tuple}
  if force || !rel.r_is_loaded
    get_relation_page_res = PagerWrap.get_page(rel.r_pager_client, rel.r_id, rel.r_cloud_bucket)
    if !PagerWrap.IsSuccess(get_relation_page_res)
      err = PagerWrap.GetResultError(get_relation_page_res)
      error("Failed to download the (table, column) = ($(rel.r_name)) from the Pager with error: $(PagerWrap.GetRawMessage(err))")
    end
    ret_relation_page = GetConstResult(get_relation_page_res)
    
    rel.r_memory_data = deserialize_cloud_relation(rel, ret_relation_page)
    
    rel.r_is_loaded = true
  end
end

function serialize_cloud_relation(rel::CloudRelation{T}) where {T <: Tuple}
  write_iob = IOBuffer()
  serialize(write_iob, rel.r_memory_data.r_columns)
  serialize(write_iob, rel.r_memory_data.r_num_keys)
  serialize(write_iob, rel.r_memory_data.r_indexes)
  seekstart(write_iob)
  relation_page_content_array = read(write_iob)
  debug(logger, "Storing relation_page_content_array::$(typeof(relation_page_content_array)) with len = $(length(relation_page_content_array)) for rel.r_name=$(rel.r_name) and rel.r_id=$(rel.r_id)")
  relation_page_content_array
end

function store_rel!(rel::CloudRelation{T}, force::Bool=false) where {T <: Tuple}
  if force || (rel.r_is_dirty && rel.r_is_persistent)
    # serialize keys and values
    columns_content = serialize_cloud_relation(rel);
    
    # create and upload the page
    relation_page = PagerWrap.create_page(rel.r_id, columns_content, convert(UInt64, length(columns_content)))
    put_relation_page_res = PagerWrap.put_page(rel.r_pager_client, relation_page, "store_rel", rel.r_cloud_bucket)
    
    # check the success and handle the error is necessary
    if !PagerWrap.IsSuccess(put_relation_page_res)
      err = PagerWrap.GetResultError(put_relation_page_res)
      error("Failed to upload the (table, column) = ($(rel.r_name)) to the Pager with error: $(PagerWrap.GetRawMessage(err))")
    end
    rel.r_is_dirty = false
  end
end

@inline function get_rel_index!(f::Function, rel::CloudRelation{T}, key) where {T <: Tuple}
  load_rel!(rel)
  get_rel_index!(f, rel.r_memory_data, key)
end

@inline function get_rel_columns(rel::CloudRelation{T}) where {T <: Tuple}
  load_rel!(rel)
  get_rel_columns(rel.r_memory_data)
end

@inline function get_rel_column(rel::CloudRelation{T}, idx::Int) where {T <: Tuple}
  load_rel!(rel)
  get_rel_column(rel.r_memory_data, idx)
end

@inline function get_rel_num_keys(rel::CloudRelation{T}) where {T <: Tuple}
  load_rel!(rel)
  get_rel_num_keys(rel.r_memory_data)
end

@inline function get_rel_index(rel::CloudRelation{T}, key) where {T <: Tuple}
  load_rel!(rel)
  get_rel_index(rel.r_memory_data, key)
end

function replace!(old::CloudRelation{T}, new::CloudRelation{T}) where {T <: Tuple}
  load_rel!(new)
  replace!(old.r_memory_data, new.r_memory_data)
end

function create_cloud_relation(::Type{T}, cloud_bucket::String, pager_client::PagerWrap.PagerClient, num_keys::Int, rel_id::UInt64, is_persistent::Bool=true, rel_name::String="", memory_data::Union{MemoryRelation{T}, Void}=nothing, force_save_if_data_available::Bool=true) where {T <: Tuple}
  is_loaded = memory_data != nothing # if the data is available, then it's loaded
  is_dirty = is_loaded #if the data is directly provided, it means that we should upload it to the cloud
  rel = CloudRelation{T}(rel_name, rel_id, is_loaded, is_dirty, is_persistent, num_keys, memory_data, pager_client, cloud_bucket)
  @assert !is_persistent || (rel_id > typemin(UInt64))
  if force_save_if_data_available && is_dirty
    store_rel!(rel)
  end
  rel
end

function create_relation(columns::T, num_keys::Int=length(columns)-1, indexes::Union{Dict{Vector{Int},T}, Void}=nothing) where {T <: Tuple}
  if(indexes == nothing)
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
  else
    MemoryRelation{T}(columns, num_keys, indexes)
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

export Relation, create_relation, create_cloud_relation, get_rel_index, get_rel_column, get_rel_columns, get_rel_num_keys, @relation, index, parse_relation, empty, diff

end
