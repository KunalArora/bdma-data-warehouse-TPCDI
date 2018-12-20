defmodule FactHolding do
  @moduledoc """
  Documentation for FactHolding.
  """

  @doc """
  Hello world.

  ## Examples

      iex> FactHolding.hello()
      :world

  """
  defp dbConnect(username, password, database) do
    Mariaex.start_link(username: username, password: password, database: database)
  end



  def readCSV(csvFile, fieldSeparator) do
    hh_rows = File.stream!(csvFile) |> 
        CSV.decode!(separator: fieldSeparator) |> 
        Enum.map(&(&1))
    hh_col = %{"TradeID" => 0, "CurrentTradeID" => 1, "BeforeQTY" => 2, "CurrentHolding" => 3}
    
    {:ok, db_conn} = Mariaex.start_link(username: "DW", password: "DW",database: "tpcdi")

    {:ok, result} = Mariaex.query(db_conn, "select * from DimTrade;")
    {:ok, col_list} = Map.fetch(result, :columns)
    {:ok, trade_rows} = Map.fetch(result, :rows)
    trade_col = col_list |> 
      Enum.chunk_every(1) |>
      Enum.into(%{}, fn x -> {hd(x), Enum.find_index(col_list, &([&1]==x))} end)

    key = "TradeID"
    trade_map = trade_rows |>
      Enum.into(%{}, fn row -> {
        elem(Integer.parse(Enum.at(row, Map.fetch!(trade_col, key))), 0), 
        List.delete_at(row, Map.fetch!(trade_col, key))
      } end)
    col_list = col_list |> List.delete_at(1)
    trade_col = col_list |> 
      Enum.chunk_every(1) |>
      Enum.into(%{}, fn x -> {hd(x), Enum.find_index(col_list, &([&1]==x))} end)

    {:ok, table} = Table.start_link([])
    import Enum
    for hh_row <- hh_rows do
      key = at(hh_row, Map.fetch!(hh_col, "TradeID"))
      tradeID = elem(Integer.parse(key), 0)
      if Map.has_key?(trade_map, tradeID) do
        {:ok, trade_row} = Map.fetch(trade_map, tradeID)
        row = [
          at(hh_row, Map.fetch!(hh_col, "CurrentTradeID")),
          at(hh_row, Map.fetch!(hh_col, "TradeID")),
          at(trade_row, Map.fetch!(trade_col, "SK\_CustomerID")),
          at(trade_row, Map.fetch!(trade_col, "SK\_AccountID")),
          at(trade_row, Map.fetch!(trade_col, "SK\_SecurityID")),
          at(trade_row, Map.fetch!(trade_col, "SK\_CompanyID")),
          at(trade_row, Map.fetch!(trade_col, "TradePrice")),
          at(trade_row, Map.fetch!(trade_col, "SK\_CloseDateID")),
          at(trade_row, Map.fetch!(trade_col, "SK\_CloseTimeID")),
          at(hh_row, Map.fetch!(hh_col, "CurrentHolding")),
          1
        ]
        Table.put(table, [row])
      end
    end
    Table.get(table)
    # holdings = Table.get(table)--[]
    # for holding <- holdings do
    #   insert_query = "insert into FactHoldings values "

    #   {:ok, result} = db_conn |> Mariaex.query(insert_query)
    # end
  end

end
