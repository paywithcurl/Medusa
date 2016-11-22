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

  def handle_info({:retry, %Message{body: body, metadata: metadata}}, state) do
    max_retry = Medusa.config |> Keyword.get(:retry_consumer_max, 10)
    if Map.get(metadata, "retry", 1) < max_retry do
      metadata =
        metadata
        |> Map.delete("channel")
        |> Map.delete("id")
        |> Map.delete("delivery_tag")
      Medusa.Broker.publish(metadata["event"], body, metadata)
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
        try do
          case f.(event) do
            :error -> retry_event(event)
            {:error, _} -> retry_event(event)
            _ -> :ok
          end
        rescue
          _ -> retry_event(event)
        catch
          _ -> retry_event(event)
        end
        AMQP.Basic.ack(chan, tag)
      end
    end)
    if opts[:bind_once] do
      {:stop, :normal, state}
    else
      {:noreply, [], state}
    end
  end

  defp retry_event(%Message{} = message) do
    message = update_in(message,
                        [Access.key(:metadata), "retry"],
                        &((&1 || 0) + 1))
    time =
      Medusa.config
      |> Keyword.get(:retry_consumer_pow, 2)
      |> :math.pow(message.metadata["retry"])
      |> round
      |> :timer.seconds
    Process.send_after(self, {:retry, message}, time)
  end

end
