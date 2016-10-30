defmodule Boltex.Server do

  use GenServer
  alias Boltex.Bolt

  @default_options [host: "localhost", port: 7687, user: "neo4j", password: "neo4j"]
  @connect_mode [active: false, mode: :binary, packet: :raw]

  # OTP

  def start_link, do: start_link(__MODULE__, @default_options)
  def start_link(name), do: start_link(name, Application.get_env(:boltex, name))
  def start_link(name, options) when is_atom(name) do
    GenServer.start_link(__MODULE__, options, name: name)
  end

  def init(options) do
    {:ok, {:disconnected, options}}
  end

  def handle_call({:connect, more_opts}, _from, {:disconnected, options}) do
    {:ok, port} = Keyword.merge(options, more_opts) |> connect
    {:reply, :ok, port}
  end

  def handle_call(call, from, {:disconnected, options}) do
    {:ok, port} = connect(options)
    handle_call(call, from, port)
  end

  def handle_call({:run, statement}, _from, port) when is_port(port) do
    result = Bolt.run_statement(:gen_tcp, port, statement)
    {:reply, result, port}
  end

  # Privates

  defp string_to_charlist(s) when is_binary(s), do: String.to_charlist(s)
  defp string_to_charlist(s), do: s

  defp connect(options) do
    %{host: host, port: port, user: user, password: password} =
      Keyword.merge(@default_options, options) |> Enum.into(%{})
    {:ok, port} = :gen_tcp.connect string_to_charlist(host), port, @connect_mode
    :ok = Bolt.handshake :gen_tcp, port
    :ok = Bolt.init :gen_tcp, port, {user, password}
    {:ok, port}
  end

end
