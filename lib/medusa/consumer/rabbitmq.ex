alias Experimental.GenStage

defmodule Medusa.Consumer.RabbitMQ do
  use GenStage
  require Logger
  alias Medusa.Broker.Message

  def start_link(args) do
    to_link =
      args
      |> get_in([:opts, :queue_name])
      |> String.to_atom
      |> Process.whereis
    params = %{
      function: Keyword.fetch!(args, :function),
      to_link: to_link,
      opts: Keyword.get(args, :opts, [])
   }
    GenStage.start_link(__MODULE__, params)
  end

  def init(%{to_link: link} = params) do
    Logger.debug("Starting #{__MODULE__} for: #{inspect params}")
    {:consumer, params, subscribe_to: [link]}
  end

  @doc """
  Process the event passing the argument to the function.
  """
  def handle_events(events, _from, state) do
    Logger.debug("#{__MODULE__} Received event: #{inspect events}")
    events
    |> List.flatten
    |> do_handle_events(state)
  end

  def handle_info({:retry, %Message{} = message}, state) do
    do_event(message, state.function, state)
    {:noreply, [], state}
  end

  def terminate(reason, state) do
    Logger.error("""
      #{__MODULE__}
      state: #{inspect state}
      die: #{inspect reason}
    """)
  end

  defp do_handle_events([], state) do
    {:noreply, [], state}
  end

  defp do_handle_events(events, %{function: f, opts: opts} = state) do
    Enum.each(events, fn event ->
      with %AMQP.Channel{} <- event.metadata["channel"],
           tag when is_number(tag) <- event.metadata["delivery_tag"] do
        do_event(event, f, state)
      end
    end)
    if opts[:bind_once] do
      {:stop, :normal, state}
    else
      {:noreply, [], state}
    end
  end

  defp do_event(%Message{} = message, [function], state) do
    try do
      case function.(message) do
        :ok -> ack_message(message)
        :error -> retry_event(message, state)
        {:error, _} -> retry_event(message, state)
        _ -> drop_message(message)
      end
    rescue
      _ -> retry_event(message, state)
    catch
      _ -> retry_event(message, state)
    end
  end

  defp do_event(%Message{} = message, [function|tail], state) do
    try do
      case function.(message) do
        new_message = %Message{} -> do_event(new_message, tail, state)
        _ -> drop_or_requeue_message(message, state)
      end
    rescue
      _ -> drop_or_requeue_message(message, state)
    catch
      _ -> drop_or_requeue_message(message, state)
    end
  end

  defp retry_event(%Message{metadata: metadata} = message, state) do
    max_retries = state.opts[:max_retries] || 1
    message = update_in(message,
                        [Access.key(:metadata), "retry"],
                        &((&1 || 0) + 1))
    if metadata["retry"] <= max_retries do
      time = 2 |> :math.pow(metadata["retry"]) |> round |> :timer.seconds
      Process.send_after(self, {:retry, message}, time)
    else
      Logger.warn("Failed processing message #{inspect message}")
      drop_or_requeue_message(message, state)
    end
  end

  defp drop_or_requeue_message(%Message{} = message, state) do
    if state.opts[:drop_on_failure] do
      drop_message(message)
    else
      requeue_message(message)
    end
  end

  defp ack_message(%Message{metadata: metadata}) do
    AMQP.Basic.ack(metadata["channel"], metadata["delivery_tag"])
  end

  defp requeue_message(%Message{metadata: metadata} = message) do
    Logger.warn("Requeueing message #{inspect message}")
    AMQP.Basic.nack(metadata["channel"],
                    metadata["delivery_tag"],
                    requeue: true)
  end

  defp drop_message(%Message{metadata: metadata} = message) do
    Logger.warn("Dropping message #{inspect message}")
    AMQP.Basic.nack(metadata["channel"],
                    metadata["delivery_tag"],
                    requeue: false)
  end

end
