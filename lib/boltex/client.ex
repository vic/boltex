defmodule Boltex.Client do

  defmacro __using__(options) do
    quote do
      @behaviour Boltex.Client

      def connect(options \\ unquote(options)),
        do: Boltex.Client.perform(__MODULE__, {:connect, options})

      def run(statement, params \\ %{}),
        do: Boltex.Client.perform(__MODULE__, {:run, {statement, params}})

    end
  end

  @callback connect() :: :ok | any
  @callback connect(Keyword.t) :: :ok | any

  @callback run(String.t) :: any
  @callback run(String.t, Map.t) :: any

  @doc false
  def perform(server, {:run, statement}) do
    GenServer.call(server, {:run, statement})
    |> statement_result
  end

  def perform(server, request), do: GenServer.call(server, request)

  defp statement_result(failure: failure) do
    {:error, failure}
  end

  defp statement_result(success: %{"fields" => fields, record: record}) do
    value = Enum.zip(fields, record) |> Enum.into(%{})
    {:ok, value}
  end

end
