defmodule Boltex.Server do

  use GenServer
  alias Boltex.Bolt

  @transport :gen_tcp
  @transport_init [active: false, mode: :binary, packet: :raw]
  @default_options [host: "localhost", port: 7687, user: "neo4j", password: "neo4j"]

  # OTP

  def start_link, do: start_link(__MODULE__, @default_options)
  def start_link(name), do: start_link(name, Application.get_env(:boltex, name))
  def start_link(name, options) when is_atom(name) do
    GenServer.start_link(__MODULE__, options, name: name)
  end

  def init(conn_opts) do
    {:ok, {:disconnected, conn_opts}}
  end

  def handle_call(:disconnect, _from, st = {:disconnected, _}) do
    {:reply, :ok, st}
  end

  def handle_call(:disconnect, _from, {:open, port, conn_opts}) do
    :ok = @transport.close(port)
    {:reply, :ok, {:disconnected, conn_opts}}
  end

  def handle_call({:connect, more_opts}, _from, st = {:open, _, _}) do
    {:reply, :ok, st}
  end

  def handle_call({:connect, more_opts}, from, {:disconnected, options}) do
    conn_opts = Keyword.merge(options, more_opts) 
    {:reply, :ok, connect(conn_opts)}
  end

  def handle_call(call, from, {:disconnected, conn_opts}) do
    handle_call(call, from, connect(conn_opts))
  end

  def handle_call({:run, {statement, params}}, _from, open = {:open, port, _}) do
    result = Bolt.run_statement(@transport, port, statement, params)
    case result do
      [{:success, _} | _] ->
        {:reply, result, open}
      {:failure, _} ->
        {:reply, result, open}
    end
  end

  # Privates

  defp string_to_charlist(s) when is_binary(s), do: String.to_charlist(s)
  defp string_to_charlist(s), do: s

  defp connect(conn_opts) do
    %{host: host, port: port, user: user, password: password} =
      Keyword.merge(@default_options, conn_opts) |> Enum.into(%{})
    {:ok, port} = @transport.connect string_to_charlist(host), port, @transport_init
    :ok = Bolt.handshake @transport, port
    :ok = Bolt.init @transport, port, {user, password}
    {:open, port, conn_opts}
  end

end
