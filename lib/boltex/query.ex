defmodule Boltex.Query do
  defstruct statement: ""
end


defimpl DBConnection.Query, for: Boltex.Query do
  alias Boltex.Query

  def parse(query, _), do: query

  def encode(query, data, _), do: data

  def decode(_, result, _), do: result
end
