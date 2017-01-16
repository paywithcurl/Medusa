alias Experimental.GenStage

defmodule Medusa.Consumer.RabbitMQ do
  use GenStage
  require Logger
  alias Medusa.Message

  def start_link(args) do
    Logger.metadata(service: System.get_env("SERVICE_NAME"))
    to_link =
      args |> get_in([:opts, :queue_name]) |> String.to_atom |> Process.whereis
    params = %{
      function: Keyword.fetch!(args, :function) |> List.wrap,
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
    events |> List.flatten |> do_handle_events(state)
  end

  def handle_info({:retry, %Message{} = message}, state) do
    do_event(message, state.function, message, state)
    {:noreply, [], state}
  end

  def handle_info(msg, state) do
    Logger.warn("Got unexpected message #{inspect msg} state #{inspect state} from #{inspect self}")
    {:noreply, state}
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
      metadata = event.metadata
      with %AMQP.Channel{} <- metadata["channel"],
           tag when is_number(tag) <- metadata["delivery_tag"],
           validators <- Keyword.get(opts, :message_validators, []),
           :ok <- Medusa.validate_message(validators,
                                          event) do
        do_event(event, f, event, state)
      else
        {:error, reason} ->
          Logger.warn "Message failed validation #{inspect reason}: #{inspect event}"
          {:error, "message is invalid"}
      end
    end)
    if opts[:bind_once] do
      {:stop, :normal, state}
    else
      {:noreply, [], state}
    end
  end

  defp do_event(%Message{} = message, [function], original_message, state) do
    try do
      IO.inspect(message)
      IO.inspect(original_message)
      Logger.warn("medusa.event.processed", [message_id: message.body[:id], topic: message.topic])

      case function.(message) do
        :ok -> ack_message(original_message)
        :error ->
          Logger.error("Error processing message #{inspect function}")
          retry_event(original_message, state)
        {:error, reason} ->
          Logger.error("Error processing message #{inspect function} #{inspect reason}")
          retry_event(original_message, state)
        error ->
          Logger.error("Error processing message #{inspect function} #{inspect error}")
          drop_message(original_message)
      end
    rescue
      error ->
        Logger.error("Error processing message #{inspect function} #{inspect error}")
        retry_event(original_message, state)
    catch
      error ->
        Logger.error("Error processing message #{inspect function} #{inspect error}")
        retry_event(original_message, state)
      error, other_error ->
        Logger.error("Error processing message #{inspect function} #{inspect error} #{inspect other_error}")
        retry_event(original_message, state)
    end
  end

  defp do_event(%Message{} = message, [function|tail], original_message, state) do
    try do
      Logger.info("medusa.event.processed", [message_id: message.body[:id], topic: message.topic])

      case function.(message) do
        new = %Message{} -> do_event(new, tail, original_message, state)
        error ->
          Logger.error("Error processing message #{inspect function} #{inspect error}")
          drop_or_requeue_message(original_message, state)
      end
    rescue
      error ->
        Logger.error("Error processing message #{inspect function} #{inspect error}")
        drop_or_requeue_message(original_message, state)
    catch
      error ->
        Logger.error("Error processing message #{inspect function} #{inspect error}")
        drop_or_requeue_message(original_message, state)
      error, other_error ->
        Logger.error("Error processing message #{inspect function} #{inspect error} #{inspect other_error}")
        drop_or_requeue_message(original_message, state)
    end
  end

  defp retry_event(%Message{} = message, state) do
    max_retries = state.opts[:max_retries] || 1
    message = update_in(message,
                        [Access.key(:metadata), "retry"],
                        &((&1 || 0) + 1))
    if message.metadata["retry"] <= max_retries do
      base = Medusa.config |> Keyword.get(:retry_consume_pow_base, 2)
      time = base |> :math.pow(message.metadata["retry"]) |> round |> :timer.seconds
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
