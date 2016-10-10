module Minesweeper

using Data
using Query
using UI
using Match
using Blink
using Hiccup
@tags button

function count(relation)
  length(relation[1])
end

macro exists(clause)
  quote 
    exists = @query begin 
      exists = true
      $clause
      return (exists::Bool,)
    end
    length(exists[1]) == 1
  end
end

function fix(flow, relation)
  order = collect(1:length(relation))
  while true
    old_state = relation.columns
    flow()
    new_state = relation.columns
    if new_state == old_state
      return
    end
  end
end

function run(num_x, num_y, num_mines)
  
  @relation state() => Symbol
  @relation mine(Int64, Int64)
  @relation mine_count(Int64, Int64) => Int64
  @relation cleared(Int64, Int64)
  @relation clicked(Int64) => (Int64, Int64)
  
  @relation cell(Int64, Int64) => Hiccup.Node
  @relation row(Int64) => Hiccup.Node
  @relation grid() => Hiccup.Node
  
  @query begin 
    return state() => :game_in_progress
  end
  
  while length(mine[1]) < num_mines
    @query begin 
      nx = rand(1:num_x)
      ny = rand(1:num_y)
      return mine(nx, ny)
    end 
  end
  
  @query begin 
    x in 1:num_x
    y in 1:num_y
    c = count(@query begin
      nx in (x-1):(x+1)
      ny in (y-1):(y+1)
      @when (nx != x) || (ny != y)
      mine(nx, ny) 
    end)
    return mine_count(x, y) => c
  end
  
  @Window(clicked) do window, event_number

    @query begin
      clicked($event_number) => (x, y)
      return cleared(x, y)
    end
    
    fix(cleared) do
      @query begin
        cleared(x,y)
        mine_count(x,y) => 0
        nx in (x-1):(x+1)
        ny in (y-1):(y+1)
        @when nx in 1:num_x
        @when ny in 1:num_y
        @when (nx != x) || (ny != y)
        return cleared(nx, ny)
      end
    end
    
    @query begin 
      num_cleared = count(@query begin 
        cleared(x,y)
        return (x::Int64, y::Int64)
      end)
      @when num_cleared + num_mines >= num_x * num_y
      return state() => :game_won
    end
    
    @query begin
      clicked($event_number) => (x, y)
      mine(x,y)
      return state() => :game_lost
    end
    
    @query begin
      state() => current_state
      x in 1:num_x
      y in 1:num_y
      is_cleared = @exists cleared($x,$y)
      is_mine = @exists mine($x,$y)
      mine_count(x, y) => count
      cell_node = @match (current_state, is_mine, is_cleared, count) begin
        (:game_in_progress, _, true, 0) => button("_")
        (:game_in_progress, _, true, _) => button(string(count))
        (:game_in_progress, _, false, _) => button(Dict(:onclick => @event clicked(x,y)), "X")
        (:game_won, true, _, _) => button("💣")
        (:game_lost, true, _, _) => button("☀")
        (_, false, _, _) => button(string(count))
        other => error("The hell is this: $other")
      end
      return cell(x,y) => cell_node
    end
    
    @query begin
      y in 1:num_y
      row_node = hbox((@query cell(x,$y) => cell_node)[3])
      return row(y) => row_node
    end
    
    @query begin
      grid_node = vbox((@query row(y) => row_node)[2])
      return grid() => grid_node
    end
    
    Blink.body!(window, grid[1][1])
    
  end
  
  (state, mine, mine_count, cleared, clicked, cell, row, grid)
end

(state, mine, mine_count, cleared, clicked, cell, row, grid) = run(30, 30, 100)


end
