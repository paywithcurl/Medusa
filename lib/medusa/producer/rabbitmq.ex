alias Experimental.GenStage

defmodule Medusa.Producer.RabbitMQ do
  use GenStage
  require Logger
  alias Medusa.Message
  alias Medusa.Adapter.RabbitMQ, as: Adapter

  defmodule State do
    defstruct [
      demand: 0,
      channel: nil,
      consumer_tag: nil,
      consumers: MapSet.new(),
      topic: nil,
      queue_name: nil
    ]
  end

  def start_link(opts) do
    topic = Keyword.fetch!(opts, :name)
    queue_name = Keyword.fetch!(opts, :queue_name)
    GenStage.start_link(__MODULE__,
                        {topic, queue_name},
                        name: String.to_atom(queue_name))
  end

  def init({topic, queue_name}) do
    Logger.debug("Starting Producer #{__MODULE__} for: #{topic}")
    state = %State{topic: topic, queue_name: queue_name}
    {:producer, state, dispatcher: GenStage.DemandDispatcher}
  end

  def handle_demand(demand, state) do
    Logger.debug("#{__MODULE__} handle_demand: #{inspect demand}")
    get_next_event(%{state | demand: demand})
  end

  def handle_subscribe(:consumer, _opts, from, %{consumers: consumers} = state) do
    new_consumers = MapSet.put(consumers, from)
    {:automatic, %{state | consumers: new_consumers}}
  end

  def handle_cancel({:down, :normal}, from, %{consumers: consumers} = state) do
    Logger.debug("#{__MODULE__} handle_cancel: normal")
    new_consumers = MapSet.delete(consumers, from)
    case MapSet.size(new_consumers) do
      0 -> {:stop, :normal, state}
      _ -> {:noreply, [], %{state | consumers: new_consumers}}
    end
  end

  def handle_cancel(reason, from, %{consumers: consumers} = state) do
    Logger.debug("#{__MODULE__} handle_cancel: #{inspect reason}")
    new_consumers = MapSet.delete(consumers, from)
    {:noreply, [], %{state | consumers: new_consumers}}
  end

  def handle_info({:basic_consume_ok, meta}, state) do
    Logger.debug("#{__MODULE__} basic_consume_ok: #{inspect meta}")
    {:noreply, [], state}
  end

  def handle_info({:basic_cancel, meta}, state) do
    Logger.debug("#{__MODULE__} basic_cancel: #{inspect meta}")
    {:noreply, [], state}
  end

  def handle_info({:basic_cancel_ok, meta}, state) do
    Logger.debug("#{__MODULE__} basic_cancel_ok: #{inspect meta}")
    {:stop, :normal, state}
  end

  def handle_info({:basic_deliver, payload, meta}, state) do
    case Poison.decode(payload) do
      {:ok, msg} ->
        message = %Message{topic: msg["topic"],
                           body: msg["body"],
                           metadata: msg["metadata"]}
        message_info = %Message.Info{
          channel: state.channel,
          delivery_tag: meta[:delivery_tag],
          routing_key: meta[:routing_key],
          consumer_tag: meta[:consumer_tag],
          message_id: meta[:message_id],
          exchange: meta[:exchange]}
        new_metadata = Map.put(message.metadata, "message_info", message_info)
        message = %{message | metadata: new_metadata}
        {:noreply, [message], %{state | demand: state.demand - 1}}
      _ ->
        AMQP.Basic.reject(state.channel, meta[:delivery_tag], requeue: false)
        {:noreply, [], state}
    end
  end

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state) do
    new_channel = setup_channel(state.channel, state.topic, state.queue_name)
    state
    |> Map.put(:channel, new_channel)
    |> Map.put(:consumer_tag, nil)
    |> get_next_event
  end

  def handle_info(msg, state) do
    Logger.debug("Got unexpected message #{inspect msg} state #{inspect state} from #{inspect self()}")
    {:noreply, state}
  end

  def terminate(reason, state) do
    ensure_channel_closed(state.channel)
    Logger.debug("""
      #{__MODULE__}
      state: #{inspect state}
      die: #{inspect reason}
    """)
  end

  defp get_next_event(%State{channel: nil} = state) do
    channel = setup_channel(state.channel, state.topic, state.queue_name)
    get_next_event(%{state | channel: channel})
  end

  defp get_next_event(%State{demand: demand} = state) when demand > 0 do
    tag = state.consumer_tag || consume(state.channel, state.queue_name)
    {:noreply, [], %{state | consumer_tag: tag}}
  end

  defp get_next_event(%State{} = state) do
    state.consumer_tag && AMQP.Basic.cancel(state.channel, state.consumer_tag)
    {:noreply, [], %{state | consumer_tag: nil}}
  end

  defp setup_channel(old_chan, topic, queue_name) do
    ensure_channel_closed(old_chan)
    with %AMQP.Connection{} = conn <- Adapter.connection(),
         exchange when is_binary(exchange) <- Adapter.exchange(),
         {:ok, chan} <- AMQP.Channel.open(conn),
         :ok <- AMQP.Exchange.topic(chan, exchange, durable: true),
         :ok <- AMQP.Basic.qos(chan, prefetch_count: 1),
         {:ok, _queue} <- AMQP.Queue.declare(chan, queue_name, durable: true),
         :ok <- AMQP.Queue.bind(chan, queue_name, exchange, routing_key: topic) do
      Process.monitor(chan.pid)
      chan
    else
      error ->
        Logger.debug("#{__MODULE__} setup_channel #{inspect error}")
        Process.sleep(1_000)
        setup_channel(old_chan, topic, queue_name)
    end
  end

  defp consume(channel, queue_name) do
    {:ok, consumer_tag} = AMQP.Basic.consume(channel, queue_name)
    consumer_tag
  end

  defp ensure_channel_closed(%AMQP.Channel{} = chan) do
    if Process.alive?(chan.pid) do
      AMQP.Channel.close(chan)
    end
  end

  defp ensure_channel_closed(_) do
    :ok
  end

end
