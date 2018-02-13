module JobData

# separate module because this takes a long time, don't want to rerun it every test

using Memento
using DataFrames
using Missings
using JLD
using CxxWrap
using PagerWrap

using Imp.Util
using Imp.Data

logger = Memento.config("debug"; fmt="[{level} | {name}]: {msg}")

function drop_missing_vals(keys::Vector{Union{K, Missing}}, vals::Vector{Union{V, Missing}}) ::Tuple{Vector{K}, Vector{V}} where {K, V}
  @assert length(keys) == length(vals)
  (
    K[k for (k, v) in zip(keys, vals) if !ismissing(v)],
    V[v for (k, v) in zip(keys, vals) if !ismissing(v)],
  )
end

function compress(column::Vector{T}) where {T}
  if (T <: Integer) && !isempty(column)
    minval, maxval = minimum(column), maximum(column)
    for T2 in [Int8, Int16, Int32, Int64]
      if (minval > typemin(T2)) && (maxval < typemax(T2))
        return convert(Vector{T2}, column)
      end
    end
  else
    return column
  end
end

function get_table_column_id(table_name, column_name)
  #TODO the current method of obtaining the relation ID via hashing the tuple of table
  #and column names should be replaced by reading from a repository of such mappings
  hash((table_name, column_name))
end

function create_table_column_id_mappings(table_column_names)
  table_column_ids = Dict{String, Vector{UInt64}}()
  table_column_ids_set = Set{UInt64}()
  for (table_name, column_names) in table_column_names
    for i in 2:length(column_names)
      column_name = column_names[i]
      table_column_id = get_table_column_id(table_name, column_name)
      #the vector is initialized with one zero element to align with the skipped ID column
      push!(get!(table_column_ids, table_name, [ convert(UInt64, 0) ]), table_column_id)
      @assert !in(table_column_id, table_column_ids_set) "There is another table-column with the same ID = $table_column_id"
      push!(table_column_ids_set, table_column_id)
    end
  end
  (table_column_ids, table_column_ids_set)
end

const USE_CLOUD_RELATIONS = true
const DEFAULT_BUCKET = "relationalai"

function upload_table_data_if_not_exists(pager_client, table_column_existence_map, table_column_ids, table_name, column_names, column_types)
  table_data::Union{DataFrame, Void} = nothing
  column_ids = table_column_ids[table_name]
  for i in 2:length(column_names)
    column_name = column_names[i]
    table_column_id = column_ids[i]
    table_column_exists = table_column_existence_map[table_column_id]
    if(!table_column_exists)
      debug(logger, "($table_name, $column_name) does not exist! Trying to put its data into the Pager...")
      if(table_data == nothing)
        # table data is loaded here, as it's not already loaded for another missing column
        table_data = readtable(open("../imdb/$(table_name).csv"), header=false, eltypes=column_types)
      end
      
      # read/clean/compress data for this table-column
      (keys, vals) = drop_missing_vals(table_data.columns[1], table_data.columns[i])
      keys = compress(keys)
      vals = compress(vals)
      
      create_cloud_relation(Tuple{typeof(keys), typeof(vals)}, DEFAULT_BUCKET, pager_client, 1, column_ids[i], true, "$table_name;$(column_names[i])", create_relation((keys, vals), 1))
    end
  end
end

function create_pager_client()
  options = PagerWrap.PagerOptions(DEFAULT_BUCKET)
  PagerWrap.InitPager(options)
  cloud_options = PagerWrap.GetCloudStorageOptions(options)
  cloud_storage_client = PagerWrap.CreateCloudStorageClient(cloud_options)
  PagerWrap.PagerClient(options, cloud_storage_client)
end

schema = readdlm(open("data/job_schema.csv"), ',', header=false, quotes=true, comments=false)
table_column_names = Dict()
table_column_types = Dict()
for column in 1:size(schema)[1]
  table_name, ix, column_name, column_type = schema[column, 1:4]
  if isfile("../imdb/$(table_name).csv")
    push!(get!(table_column_names, table_name, []), column_name)
    push!(get!(table_column_types, table_name, []), (column_type == "integer" ? Int64 : String))
  end
end

if USE_CLOUD_RELATIONS
  if isempty(table_column_names)
    println("Warning: source data in ../imdb not found.")
    error("Cannot load imdb data for JOB")
  end
  
  # create mapping between table-column names and their corresponding cloud relation IDs
  (table_column_ids, table_column_ids_set) = create_table_column_id_mappings(table_column_names)

  # setup cloud pager
  pager_client = create_pager_client();
  
  # check the existence of the table-columns
  table_column_existence_results = Vector{Bool}()
  table_column_ids_to_check = collect(table_column_ids_set)
  table_column_existence_operation_res = PagerWrap.pages_exist(pager_client, table_column_ids_to_check, table_column_existence_results, DEFAULT_BUCKET)
  @assert table_column_existence_operation_res
  @assert length(table_column_ids_to_check) ==  length(table_column_existence_results) "The returned result size ($(length(table_column_existence_results))) is not the same as the number of queried pages ($(length(table_column_ids_to_check)))"
  
  table_column_existence_map = Dict(zip(table_column_ids_to_check, table_column_existence_results))
    
  for (table_name, column_names) in table_column_names
    column_types = table_column_types[table_name]
    
    # table data will be loaded from local CSV files if necessary
    # this step was not necessary if we could make sure that relations are loaded apriori
    upload_table_data_if_not_exists(pager_client, table_column_existence_map, table_column_ids, table_name, column_names, column_types)
    
    # create the corresponding cloud relation for this table-column
    column_ids = table_column_ids[table_name]
    relations = [create_cloud_relation(Tuple{Vector{column_types[1]}, Vector{column_types[column_idx]}}, DEFAULT_BUCKET, pager_client, 1, column_ids[column_idx], true, "$table_name;$(column_names[column_idx])") for column_idx in 2:length(column_names)]
    fields = [Symbol(replace(column_name, "_id", "")) for column_name in column_names[2:end]]
    typs = [Symbol("T$i") for i in 2:length(column_names)]
    @eval begin
      type $(Symbol("Type_$(table_name)")){$(typs...)}
        $([:($field::$typ) for (field, typ) in zip(fields, typs)]...)
      end
      const $(Symbol(table_name)) = $(Symbol("Type_$(table_name)"))($(relations...))
      export $(Symbol(table_name))
    end
  end
else
  if !isfile("./data/imdb.jld")
    println("Warning: data/imdb.jld not found. Attempting to build from source data.")
    if isempty(table_column_names)
      println("Warning: source data in ../imdb not found.")
      error("Cannot load imdb data for JOB")
    end
    data = Dict()
    @showtime for (table_name, column_names) in table_column_names
      column_types = table_column_types[table_name]
      @show table_name column_names column_types
      frame = readtable(open("../imdb/$(table_name).csv"), header=false, eltypes=column_types)
      for i in 2:length(frame.columns)
        (keys, vals) = drop_missing_vals(frame.columns[1], frame.columns[i])
        keys = compress(keys)
        vals = compress(vals)
        data[(table_name, column_names[i])] = (keys, vals)
      end
    end
    @showtime save("./data/imdb.jld", "data", data)
  else 
    println("Loading imdb data from data/imdb.jld. This will take several minutes.")
    data = @showtime load("./data/imdb.jld", "data")
  end

  @showtime for (table_name, column_names) in table_column_names
    relations = [create_relation(data[(table_name, column_name)], 1) for column_name in column_names[2:end]]
    fields = [Symbol(replace(column_name, "_id", "")) for column_name in column_names[2:end]]
    typs = [Symbol("T$i") for i in 2:length(column_names)]
    @eval begin
      type $(Symbol("Type_$(table_name)")){$(typs...)}
        $([:($field::$typ) for (field, typ) in zip(fields, typs)]...)
      end
      const $(Symbol(table_name)) = $(Symbol("Type_$(table_name)"))($(relations...))
      export $(Symbol(table_name))
    end
  end

  # get rid off source data
  data = nothing
  gc()
end

end
