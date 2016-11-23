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

  def handle_info({:retry, %Message{metadata: metadata} = message}, state) do
    max_retry = state.opts[:max_retries] || 1
    if Map.get(metadata, "retry", 1) < max_retry do
      do_event(message, state.function)
    else
      AMQP.Basic.nack(metadata["channel"], metadata["delivery_tag"])
    end
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
      with %AMQP.Channel{} = chan <- event.metadata["channel"],
           tag when is_number(tag) <- event.metadata["delivery_tag"] do
        do_event(event, f)
      end
    end)
    if opts[:bind_once] do
      {:stop, :normal, state}
    else
      {:noreply, [], state}
    end
  end

  defp do_event(%{metadata: metadata} = message, function) do
    try do
      case function.(message) do
        :error ->
          retry_event(message)
        {:error, _} ->
          retry_event(message)
        _ ->
          AMQP.Basic.ack(metadata["channel"], metadata["delivery_tag"])
      end
    rescue
      _ -> retry_event(message)
    catch
      _ -> retry_event(message)
    end
  end

  defp retry_event(%Message{} = message) do
    message = update_in(message,
                        [Access.key(:metadata), "retry"],
                        &((&1 || 0) + 1))
    time = 2 |> :math.pow(message.metadata["retry"]) |> round |> :timer.seconds
    Process.send_after(self, {:retry, message}, time)
  end

end
