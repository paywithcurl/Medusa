defmodule Medusa.Broker do
  @moduledoc false
  alias Medusa.Queue
  alias Medusa.ProducerSupervisor, as: Producer
  alias Medusa.ConsumerSupervisor, as: Consumer

  defmodule Message do
    defstruct topic: "", body: %{}, metadata: %{}
  end

  @doc """
  Adds a new route to the broker. If there is an existing route,
  it just ignores it.
  """
  def new_route(event, function, opts) do
    Medusa.adapter.new_route(event, function, opts)
  end

  @doc """
  Sends to the matching routes the event, using the configured adapter.
  metadata keys will always convert to string
  """
  def publish(event, payload, metadata \\ %{}) do
    message = %Message{topic: event, body: payload, metadata: metadata}
    Medusa.adapter.publish(message)
  end

  @doc """
  Start producer and consumer and subscribe
  """
  def start_producer_consumer(event, function, opts) do
    {:ok, producer} = ensure_producer_started(event)
    {:ok, consumer} = Consumer.start_child(function, event, opts)
    {producer, consumer}
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
    do_route_match?(route, incoming)
  end

  defp do_route_match?([], []), do: true
  defp do_route_match?(["*"|t1], [_|t2]), do: do_route_match?(t1, t2)
  defp do_route_match?([h|t1], [h|t2]), do: do_route_match?(t1, t2)
  defp do_route_match?(_, _), do: false

  defp ensure_producer_started(event) do
    case Producer.start_child(event) do
      {:ok, pid} when is_pid(pid) -> {:ok, pid}
      {:error, {:already_started, pid}} when is_pid(pid) -> {:ok, pid}
      {:error, error} -> {:error, error}
    end
  end

end
