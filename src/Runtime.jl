module Runtime

abstract Row

macro row(name, types)
  types = types.args
  :(immutable $(esc(name)) <: Row
      $([:($(symbol("f", i))::$(types[i])) for i in 1:length(types)]...)
    end)
end

@generated cmp_by_key{R1 <: Row, R2 <: Row}(x::R1, y::R2, xkey, ykey) = begin
  xkey = xkey.parameters[1].parameters[1]
  ykey = ykey.parameters[1].parameters[1]
  @assert(length(xkey) == length(ykey))
  :(begin
      c = cmp(x.$(xkey[1]), y.$(ykey[1]))
      $([:(if c == 0; c = cmp(x.$(xkey[i]), y.$(ykey[i])) end) for i in 2:length(xkey)]...)
      c
    end)
end

@generated Base.isless{R <: Row}(x::R, y::R) = begin
  key = Val{tuple([symbol("f", i) for i in 1:length(fieldnames(R))]...)}
  :(-1 == cmp_by_key(x, y, $key, $key))
end

@generated construct{C,K}(constructor::Type{C}, key::Type{Val{K}}, value) = begin
  constructor = constructor.parameters[1]
  fields = key.parameters[1].parameters[1]
  :(begin
      $constructor($([:(value.$field) for field in fields]...))
    end)
end

@generated construct2{C,K1,K2}(constructor::Type{C}, key1::Type{Val{K1}}, key2::Type{Val{K2}}, value1, value2) = begin
  constructor = constructor.parameters[1]
  fields1 = key1.parameters[1].parameters[1]
  fields2 = key2.parameters[1].parameters[1]
  :(begin
      $constructor(
        $([:(value1.$field) for field in fields1]...),
        $([:(value2.$field) for field in fields2]...),
        )
    end)
end

dedup_sorted{X}(xs::Vector{X}) = begin
  ys = Vector{X}(0)
  last = xs[1]
  push!(ys, last)
  for x in xs
    if x != last
      push!(ys, x)
      last = x
    end
  end
  xs
end

reshape{C,K}(xs, ys, ytype::Type{C}, ykey::Type{Val{K}}) = begin
  for x in xs
    push!(ys, construct(ytype, ykey, x))
  end
end

project(xs, ytype, ykey) = begin
  ys = Vector{ytype}(0)
  reshape(xs, ys, ytype, Val{ykey})
  sort!(ys, alg=QuickSort)
  dedup_sorted(ys)
end

join_sorted_inner{X,Y,Z,XK,YK,ZK1,ZK2}(
  xs::Vector{X}, ys::Vector{Y}, ztype::Type{Z},
  xkey::Type{Val{XK}}, ykey::Type{Val{YK}}, zkey1::Type{Val{ZK1}}, zkey2::Type{Val{ZK2}}
  ) = begin
  zs = Vector{Z}(0)
  xi = 1
  yi = 1
  while (xi <= length(xs)) && (yi <= length(ys))
    x = xs[xi]
    y = ys[yi]
    c = cmp_by_key(x, y, xkey, ykey)
    if c == -1
      xi += 1
    elseif c == 1
      yi += 1
    else
      xj = xi
      yj = yi
      while (xj <= length(xs)) && (cmp_by_key(x, xs[xj], xkey, xkey) == 0)
        xj += 1
      end
      while (yj <= length(ys)) && (cmp_by_key(y, ys[yj], ykey, ykey) == 0)
        yj += 1
      end
      for xk in xi:(xj-1)
        for yk in yi:(yj-1)
          push!(zs, construct2(Z, zkey1, zkey2, xs[xk], ys[yk]))
        end
      end
      xi = xj
      yi = yj
    end
  end
  zs
end

@inline
join_sorted(xs, ys, ztype, xkey, ykey, zkey1, zkey2) =
  join_sorted_inner(xs, ys, ztype, Val{xkey}, Val{ykey}, Val{zkey1}, Val{zkey2})

semijoin_sorted_inner{X,Y,XK,YK}(
  xs::Vector{X}, ys::Vector{Y},
  xkey::Type{Val{XK}}, ykey::Type{Val{YK}}
  ) = begin
  zs = Vector{X}(0)
  xi = 1
  yi = 1
  while (xi <= length(xs)) && (yi <= length(ys))
    x = xs[xi]
    y = ys[yi]
    c = cmp_by_key(x, y, xkey, ykey)
    if c == -1
      xi += 1
    elseif c == 1
      yi += 1
    else
      push!(zs, x)
      xi += 1
    end
  end
  zs
end

@inline
semijoin_sorted(xs, ys, xkey, ykey) =
  semijoin_sorted_inner(xs, ys, Val{xkey}, Val{ykey})

read_tsv(rowtype, filename) = begin
  fieldtypes = [fieldtype(rowtype, fieldname) for fieldname in fieldnames(rowtype)]
  raw = readdlm(filename, '\t', UTF8String, header=true, quotes=false, comments=false)[1]
  results = Vector{rowtype}(0)
  for i in 1:size(raw,1)
    row = Vector{Any}(vec(raw[i,:]))
    for j in 1:length(fieldtypes)
      if issubtype(fieldtypes[j], Number)
        row[j] = parse(fieldtypes[j], row[j])
      end
    end
    push!(results, rowtype(row...))
  end
  results
end

@row(Artist, [Int64, UTF8String])
@row(Album, [Int64, UTF8String, Int64])
@row(Track, [Int64, UTF8String, Int64, Int64, Int64, UTF8String, Float64, Float64, Float64])
@row(PlaylistTrack, [Int64, Int64])
@row(Playlist, [Int64, UTF8String])

chinook() = begin
  (
    read_tsv(Artist, "code/imp/data/Artist.csv"),
    read_tsv(Album, "code/imp/data/Album.csv"),
    read_tsv(Track, "code/imp/data/Track.csv"),
    read_tsv(PlaylistTrack, "code/imp/data/PlaylistTrack.csv"),
    read_tsv(Playlist, "code/imp/data/Playlist.csv"),
    )
end

@row(I1, [Int64, UTF8String]) # playlist_id playlist_name
@row(I2, [Int64, Int64]) # playlist_id track_id
@row(I3, [Int64, Int64]) # track_id playlist_id
@row(I4, [Int64, Int64]) # track_id album_id
@row(I5, [Int64, Int64]) # album_id track_id
@row(I6, [Int64, Int64]) # album_id artist_id
@row(I7, [Int64, Int64]) # artist_id album_id
@row(I8, [Int64, UTF8String]) # artist_id artist_name
@row(I9, [Int64, UTF8String]) # album_id artist_name
@row(I10, [Int64, UTF8String]) # track_id artist_name
@row(I11, [Int64, UTF8String]) # playlist_id artist_name
@row(I12, [UTF8String, UTF8String]) # playlist_name artist_name

metal(data) = begin
  i0 = filter(row -> row.f2 == "Heavy Metal Classic", data[5])

  i1 = project(i0, I1, (1,2))
  i2 = project(data[4], I2, (1,2))
  i2s = semijoin_sorted(i2, i1, (1,), (1,))

  i3 = project(i2s, I3, (2,1))
  i4 = project(data[3], I4, (1,3))
  i4s = semijoin_sorted(i4, i3, (1,), (1,))

  i5 = project(i4s, I5, (2,1))
  i6 = project(data[2], I6, (1,3))
  i6s = semijoin_sorted(i6, i5, (1,), (1,))

  i7 = project(i6s, I7, (2,1))
  i8 = project(data[1], I8, (1,2))
  i9 = join_sorted(i7, i8, I9, (1,), (1,), (2,), (2,))

  i9s = project(i9, I9, (1,2))
  i10 = join_sorted(i5, i9s, I10, (1,), (1,), (2,), (2,))

  i10s = project(i10, I10, (1,2))
  i11 = join_sorted(i3, i10s, I11, (1,), (1,), (2,), (2,))

  i11s = project(i11, I11, (1,2))
  i12 = join_sorted(i1, i11s, I12, (1,), (1,), (2,), (2,))

  i12
end

metal(chinook())

end
