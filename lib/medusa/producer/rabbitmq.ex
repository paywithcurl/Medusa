alias Experimental.GenStage

defmodule Medusa.Producer.RabbitMQ do
  use GenStage
  require Logger
  alias Medusa.Broker.Message
  alias Medusa.Adapter.RabbitMQ, as: Adapter

  defstruct [
    demand: 0,
    channel: nil,
    consumer_tag: nil,
    topic: nil,
    queue_name: nil
  ]

  def start_link(opts) do
    topic = Keyword.fetch!(opts, :name)
    function = Keyword.fetch!(opts, :function)
    queue_name =
      opts
      |> Keyword.get(:queue_name)
      |> queue_name(topic, function)
    GenStage.start_link(__MODULE__,
                        {topic, queue_name},
                        name: String.to_atom(queue_name))
  end

  def init({topic, queue_name}) do
    Logger.debug("Starting Producer #{__MODULE__} for: #{topic}")
    state = %__MODULE__{topic: topic, queue_name: queue_name}
    {:producer, state, dispatcher: GenStage.DemandDispatcher}
  end

  def handle_demand(demand, state) do
    get_next_event(%{state | demand: demand})
  end

  def handle_subscribe(:consumer, _opts, _from, state) do
    {:automatic, state}
  end

  def handle_cancel({:down, _reason, _process}, _from, state) do
     {:noreply, [], state}
  end

  def handle_cancel(_reason, _from, state) do
    close_connection(state.channel)
    {:stop, :normal, state}
  end

  def handle_cancel(_reason, _from, state) do
    {:noreply, [], state}
  end

  def handle_info({:basic_consume_ok, _meta}, state) do
    {:noreply, [], state}
  end

  def handle_info({:basic_cancel, _meta}, state) do
    close_connection(state.channel)
    {:stop, :normal, state}
  end

  def handle_info({:basic_cancel_ok, _meta}, state) do
    {:noreply, [], state}
  end

  def handle_info({:basic_deliver, payload, %{delivery_tag: tag}}, state) do
    case Poison.decode(payload) do
      {:ok, msg} ->
        message = %Message{body: msg["body"], metadata: msg["metadata"]}
        info = %{"channel" => state.channel, "delivery_tag" => tag}
        message = Map.update(message,
                             :metadata,
                             info,
                             &Map.merge(&1, info))
        {:noreply, [message], %{state | demand: state.demand - 1}}
      _ ->
        AMQP.Basic.reject(state.channel, tag, requeue: false)
        {:noreply, [], state}
    end
  end

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state) do
    close_connection(state.channel)
    new_channel = setup_channel(state.topic, state.queue_name)
    state
    |> Map.put(:channel, new_channel)
    |> Map.put(:consumer_tag, nil)
    |> get_next_event
  end

  def terminate(reason, state) do
    Logger.error("""
      #{__MODULE__}
      state: #{inspect state}
      die: #{inspect reason}
    """)
  end

  defp get_next_event(%__MODULE__{channel: nil} = state) do
    channel = setup_channel(state.topic, state.queue_name)
    get_next_event(%{state | channel: channel})
  end

  defp get_next_event(%__MODULE__{demand: demand} = state) when demand > 0 do
    tag = state.consumer_tag || consume(state.channel, state.queue_name)
    {:noreply, [], %{state | consumer_tag: tag}}
  end

  defp get_next_event(%__MODULE__{} = state) do
    state.consumer_tag && AMQP.Basic.cancel(state.channel, state.consumer_tag)
    {:noreply, [], %{state | consumer_tag: nil}}
  end

  defp setup_channel(topic, queue_name) do
    with %AMQP.Connection{} = conn <- Adapter.connection(),
         exchange when is_binary(exchange) <- Adapter.exchange(),
         {:ok, chan} <- AMQP.Channel.open(conn),
         :ok <- AMQP.Exchange.topic(chan, exchange, durable: true),
         :ok <- AMQP.Basic.qos(chan, prefetch_count: 1),
         {:ok, _queue} <- AMQP.Queue.declare(chan, queue_name, queue_opts()),
         :ok <- AMQP.Queue.bind(chan, queue_name, exchange, routing_key: topic) do
      Process.monitor(chan.pid)
      chan
    else
      error ->
        Logger.warn("#{__MODULE__} setup_channel #{inspect error}")
        Process.sleep(1_000)
        setup_channel(topic, queue_name)
    end
  end

  defp consume(channel, queue_name) do
    {:ok, consumer_tag} = AMQP.Basic.consume(channel, queue_name)
    consumer_tag
  end

  defp close_connection(%AMQP.Channel{} = chan) do
    if Process.alive?(chan.pid) do
      AMQP.Channel.close(chan)
    end
  end

  defp group_name do
    :medusa
    |> Application.get_env(Medusa)
    |> Keyword.get(:group)
  end

  defp queue_name(name, topic, function) do
    group = group_name || random_name
    "#{group}.#{do_queue_name(name, group, topic, function)}"
  end

  defp do_queue_name(name, _, _, _) when is_binary(name) do
    name
  end

  defp do_queue_name(_, group, topic, function) do
    {group, topic, function} |> :erlang.phash2
  end

  defp random_name(len \\ 8) do
    len
    |> :crypto.strong_rand_bytes
    |> Base.url_encode64
    |> binary_part(0, len)
  end

  defp queue_opts, do: [durable: true]

end
