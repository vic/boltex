defmodule Boltex.Client do

  defmacro __using__(options) do
    quote do
      @behaviour Boltex.Client

      def connect(options \\ unquote(options)),
        do: Boltex.Client.call(__MODULE__, {:connect, options})

      def run(statement, params \\ %{}),
        do: Boltex.Client.call(__MODULE__, {:run, {statement, params}})

    end
  end

  @callback connect() :: :ok | any
  @callback connect(Keyword.t) :: :ok | any

  @callback run(String.t) :: any
  @callback run(String.t, Map.t) :: any

  @doc false
  def call(server, {:run, statement}) do
    GenServer.call(server, {:run, statement})
    |> statement_result
    |> case do
         success = {:ok, _} -> success
         error ->
           GenServer.cast(server, :reset)
           error
       end
  end

  def call(server, request), do: GenServer.call(server, request)

  defp statement_result(failure: failure) do
    {:error, failure}
  end

  defp statement_result(success: %{"fields" => fields, record: record}) do
    value = Enum.zip(fields, record) |> Enum.into(%{})
    {:ok, value}
  end

end
