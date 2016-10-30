defmodule Boltex.Client do

  defmacro __using__(options) do
    quote do
      @behaviour Boltex.Client

      def disconnect(),
        do: Boltex.Client.perform(__MODULE__, :disconnect)

      def connect(options \\ unquote(options)),
        do: Boltex.Client.perform(__MODULE__, {:connect, options})

      def run(statement, params \\ %{}),
        do: Boltex.Client.perform(__MODULE__, {:run, {statement, params}})

    end
  end

  @callback disconnect() :: :ok

  @callback connect() :: :ok | any
  @callback connect(Keyword.t) :: :ok | any

  @callback run(String.t) :: any
  @callback run(String.t, Map.t) :: any

  @doc false
  def perform(server, run = {:run, _}) do
    GenServer.call(server, run) |> statement_result
  end

  def perform(server, request), do: GenServer.call(server, request)

  defp statement_result(failure: failure) do
    {:error, failure}
  end

  defp statement_result(success: %{"fields" => fields}, record: record, success: _) do
    value = for {k, v} <- Enum.zip(fields, record), into: %{}, do: {k, statement_value(v)}
    {:ok, value}
  end

  defp statement_result(success: %{"fields" => []}, success: _) do
    :ok
  end

  defp statement_value([sig: _, fields: [id, tags, properties]]),
    do: %Boltex.Node{id: id, tags: tags, properties: properties}

  defp statement_value(x), do: x

end
