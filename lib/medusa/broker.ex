defmodule Medusa.Broker do
  @moduledoc false
  alias Medusa.Queue
  alias Medusa.ProducerSupervisor, as: Producer
  alias Medusa.ConsumerSupervisor, as: Consumer

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
