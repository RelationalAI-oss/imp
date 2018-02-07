module JobData

# separate module because this takes a long time, don't want to rerun it every test

using DataFrames
using Missings
using JLD

using Imp.Util
using Imp.Data

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
  relations = [Relation(data[(table_name, column_name)], 1) for column_name in column_names[2:end]]
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
