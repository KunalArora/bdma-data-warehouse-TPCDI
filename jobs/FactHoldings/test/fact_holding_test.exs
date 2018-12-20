defmodule FactHoldingTest do
  use ExUnit.Case
  doctest FactHolding


  test "1" do
    {:ok, db_conn} = Mariaex.start_link(username: "DW", password: "DW", database: "tpcdi")

    {:ok, result} = Mariaex.query(db_conn, "select * from DimTrade;")
    {:ok, col_list} = Map.fetch(result, :columns)
    {:ok, trade_rows} = Map.fetch(result, :rows)

    trade_col = col_list |> 
        Enum.chunk_every(1) |>
        Enum.into(%{}, fn x -> {hd(x), Enum.find_index(col_list, &([&1]==x))} end)
  end
end
