defmodule Medusa.Broker do
  @moduledoc false
  alias Medusa.Queue

  defmodule Message do
    defstruct body: %{}, metadata: %{}
  end

  @doc """
  Adds a new route to the broker. If there is an existing route,
  it just ignores it.
  """
  def new_route(event) do
    adapter.new_route(event)
  end

  @doc """
  Sends to the matching routes the event, using the configured adapter.
  """
  def publish(event, payload, metadata \\ %{}) do
    message = %Message{body: payload, metadata: metadata}
    adapter.publish(event, message)
  end

  @doc """
  Determine whether this event should be route
  """
  def maybe_route({route, event, payload}) do
    f = fn -> do_maybe_route(route, event, payload) end
    Task.Supervisor.start_child(Broker.Supervisor, f)
  end

  defp do_maybe_route(route, event, payload) do
    if route_match?(route, event) do
      enqueue(route, payload)
      trigger_producer(route)
    end
  end

  defp enqueue(route, payload) do
    Queue.insert(route, payload)
  end

  defp trigger_producer(route) do
    route
    |> String.to_atom
    |> GenServer.cast({:trigger})
  end

  defp route_match?(route, incoming)
  when is_binary(route) and is_binary(incoming) do
    route = String.split(route, ".")
    incoming = String.split(incoming, ".")
    route_match?(route, incoming)
  end
  defp route_match?([], []), do: true
  defp route_match?(["*"|t1], [_|t2]), do: route_match?(t1, t2)
  defp route_match?([h|t1], [h|t2]), do: route_match?(t1, t2)
  defp route_match?(_, _), do: false

  defp adapter do
    :medusa |> Application.get_env(Medusa) |> Keyword.fetch!(:adapter)
  end

end
