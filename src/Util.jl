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

function thread_safe_debug(logger, str)
  if(logger.level == "debug")
    ccall(:jl_,Void,(Any,), str)
  end
end

export @showtime, @splice, thread_safe_debug

end
