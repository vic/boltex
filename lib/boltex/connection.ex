defmodule Boltex.Connection do
  @moduledoc """
  DBConnection implementation for Boltex.

  Heavily inspired by
  https://github.com/elixir-ecto/db_connection/tree/master/examples/tcp_connection
  """

  use DBConnection

  alias Boltex.{Bolt, Connection, Query}

  defmodule Error do
    defexception [:function, :reason, :message]

    def exception({function, reason}) do
      message = "#{function} error: #{format_error(reason)}"
      %Error{function: function, reason: reason, message: message}
    end

    defp format_error(:closed), do: "closed"
    defp format_error(:timeout), do: "timeout"
    defp format_error(reason), do: :inet.format_error(reason)
  end

  @doc "Callback for DBConnection.connect/1"
  def connect(opts) do
    host        = Keyword.fetch!(opts, :host) |> parse_ip_or_hostname
    port        = Keyword.fetch!(opts, :port)
    auth        = Keyword.fetch!(opts, :auth)

    socket_opts = Keyword.get(opts, :socket_options, [])
    timeout     = Keyword.get(opts, :connect_timeout, 5_000)

    enforced_opts = [packet: :raw, mode: :binary, active: false]
    socket_opts   = Enum.reverse socket_opts, enforced_opts

    with {:ok, port} <- :gen_tcp.connect(host, port, socket_opts, timeout),
         :ok         <- Bolt.handshake(:gen_tcp, port),
         :ok         <- Bolt.init(:gen_tcp, port, auth),
         :ok         <- :inet.setopts(port, active: :once)
    do
      {:ok, port}
    else
      {:error, reason} ->
        {:error, Connection.Error.exception({:connect, reason})}
    end
  end

  @doc "Callback for DBConnection.checkout/1"
  def checkout(port) do
    case :inet.setopts(port, active: false) do
      :ok    -> {:ok, port}
      other  -> other
    end
  end

  @doc "Callback for DBConnection.checkin/1"
  def checkin(port) do
    case :inet.setopts(port, active: :once) do
      :ok    -> {:ok, port}
      other  -> other
    end
  end

  @doc "Callback for DBConnection.handle_execute/1"
  def handle_execute(%Query{statement: statement}, params, _opts, port) do
    case Bolt.run_statement(:gen_tcp, port, statement, params) do
      [{:success, _} | _] = data ->
        {:ok, data, port}

      other ->
        {:disconnect, other, port}
    end
  end

  def handle_cast(i) do
    IO.puts "cast #{inspect i}"
  end

  def handle_info({:tcp_closed, sock}, {sock, _} = state) do
    {:disconnect, Connection.Error.exception({:recv, :closed}), state}
  end
  def handle_info({:tcp_error, sock, reason}, {sock, _} = state) do
    {:disconnect, Connection.Error.exception({:recv, reason}), state}
  end
  def handle_info(_, state), do: {:ok, state}

  def parse_ip_or_hostname(host) when is_binary(host) do
    host = String.to_charlist host

    case :inet.parse_address(host) do
      {:ok, address}    -> address
      {:error, :einval} -> host
    end
  end
  def parse_ip_or_hostname(host) when is_tuple(host), do: host
end
