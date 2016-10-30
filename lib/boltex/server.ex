defmodule Boltex.Server do

  use GenServer
  alias Boltex.Bolt

  @default_options [host: 'localhost', port: 7687, user: 'neo4j', password: 'neo4j']
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
    {:ok, pid} = Keyword.merge(options, more_opts) |> connect
    {:reply, :ok, pid}
  end

  def handle_call(call, from, {:disconnected, options}) do
    {:ok, pid} = connect(options)
    handle_call(call, from, pid)
  end

  def handle_call({:run, statement}, _from, pid) when is_pid(pid) do
    result = Bolt.run_statement(:gen_tcp, pid, statement)
    {:reply, result, pid}
  end

  # Privates

  defp string_to_charlist(s) when is_binary(s), do: String.to_charlist(s)
  defp string_to_charlist(s), do: s

  defp connect(options) do
    %{host: host, port: port, user: user, password: password} =
      Keyword.merge(@default_options, options) |> Enum.into(%{})
    [host, user, password] = [host, user, password] |> Enum.map(&string_to_charlist/1)
    {:ok, pid} = :gen_tcp.connect host, port, @connect_mode
    :ok = Bolt.handshake :gen_tcp, pid
    :ok = Bolt.init :gen_tcp, pid, {user, password}
    {:ok, pid}
  end

end
