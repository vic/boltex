defmodule Boltex.Client do

  defmacro __using__(options) do
    quote do
      @behaviour Boltex.Client

      def connect(options \\ unquote(options)), do: GenServer.call(__MODULE__, {:connect, options})

      def run(statement) when is_binary(statement) do
        GenServer.call(__MODULE__, {:run, statement})
      end

    end
  end

  @callback connect() :: :ok | any
  @callback connect(options) :: :ok | any
  @callback run(String.t) :: any

end
