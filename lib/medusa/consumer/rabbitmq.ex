alias Experimental.GenStage

defmodule Medusa.Consumer.RabbitMQ do
  use GenStage
  require Logger
  alias Medusa.Message

  defmodule State do
    defstruct callback: nil, producer: nil, opts: []
  end

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  def init(opts) do
    producer = find_producer_pid_from_opts(opts)
    state = %State{
      callback: Keyword.fetch!(opts, :function) |> List.wrap,
      producer: find_producer_pid_from_opts(opts),
      opts: Keyword.get(opts, :opts, []) |> add_default_options()
    }
    Logger.debug("Starting #{__MODULE__} for: #{inspect state}")
    {:consumer, state, subscribe_to: [producer]}
  end

  @doc """
  Process the event passing the argument to the function.
  """
  def handle_events(events, _from, state) do
    Logger.debug("#{__MODULE__} Received event: #{inspect events}")
    events |> List.flatten |> do_handle_events(state)
  end

  def handle_info({:retry, %Message{} = message}, state) do
    do_event(message, state.callback, message, state)
    {:noreply, [], state}
  end

  def handle_info(msg, state) do
    Logger.warn("Got unexpected message #{inspect msg} state #{inspect state} from #{inspect self()}")
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

  defp do_handle_events(events, %{callback: f, opts: opts} = state) do
    Enum.each(events, fn event ->
      metadata = event.metadata
      with %AMQP.Channel{} <- metadata["channel"],
           tag when is_number(tag) <- metadata["delivery_tag"],
           validators = opts.message_validators,
           :ok <- Medusa.validate_message(validators, event) do
        do_event(event, f, event, state)
      else
        {:error, reason} ->
          Logger.warn "Message failed validation #{inspect reason}: #{inspect event}"
          {:error, "message is invalid"}
      end
    end)
    respone_handle_events(state)
  end

  defp do_event(%Message{} = message, [callback], original_message, state) do
    try do
      case callback.(message) do
        :ok -> ack_message(original_message)
        :error ->
          Logger.error("Error processing message #{inspect callback}")
          retry_event(original_message, state)
        {:error, reason} ->
          Logger.error("Error processing message #{inspect callback} #{inspect reason}")
          retry_event(original_message, state)
        error ->
          Logger.error("Error processing message #{inspect callback} #{inspect error}")
          drop_message(original_message)
      end
    rescue
      error ->
        Logger.error("Error processing message #{inspect callback} #{inspect error}")
        retry_event(original_message, state)
    catch
      error ->
        Logger.error("Error processing message #{inspect callback} #{inspect error}")
        retry_event(original_message, state)
      error, other_error ->
        Logger.error("Error processing message #{inspect callback} #{inspect error} #{inspect other_error}")
        retry_event(original_message, state)
    end
  end

  defp do_event(%Message{} = message, [callback|tail], original_message, state) do
    try do
      case callback.(message) do
        new = %Message{} -> do_event(new, tail, original_message, state)
        error ->
          Logger.error("Error processing message #{inspect callback} #{inspect error}")
          drop_or_requeue_message(original_message, state)
      end
    rescue
      error ->
        Logger.error("Error processing message #{inspect callback} #{inspect error}")
        drop_or_requeue_message(original_message, state)
    catch
      error ->
        Logger.error("Error processing message #{inspect callback} #{inspect error}")
        drop_or_requeue_message(original_message, state)
      error, other_error ->
        Logger.error("Error processing message #{inspect callback} #{inspect error} #{inspect other_error}")
        drop_or_requeue_message(original_message, state)
    end
  end

  defp retry_event(%Message{} = message, %{opts: %{max_retries: max_retries}} = state) do
    message = update_in(message.metadata["retry"], &((&1 || 0) + 1))
    if message.metadata["retry"] <= max_retries do
      base = Medusa.config |> Keyword.get(:retry_consume_pow_base, 2)
      time = base |> :math.pow(message.metadata["retry"]) |> round |> :timer.seconds
      Process.send_after(self(), {:retry, message}, time)
    else
      Logger.warn("Failed processing message #{inspect message}")
      drop_or_requeue_message(message, state)
    end
  end

  defp respone_handle_events(%State{opts: %{bind_once: true}} = state) do
    {:stop, :normal, state}
  end

  defp respone_handle_events(%State{opts: %{bind_once: false}} = state) do
    {:noreply, [], state}
  end

  defp drop_or_requeue_message(
      %Message{} = message,
      %{opts: %{on_failure: :drop}}) do
    drop_message(message)
  end

  defp drop_or_requeue_message(
      %Message{} = message,
      %{opts: %{on_failure: :keep}}) do
    requeue_message(message)
  end

  defp drop_or_requeue_message(
      %Message{} = message,
      %{opts: %{on_failure: callback}}) when is_function(callback, 1) do
      case callback.(message) do
        :drop ->
          drop_message(message)
        :keep ->
          requeue_message(message)
        error ->
          Logger.error("#{__MODULE__} expected on_failure function to return [:drop, :keep]. got #{inspect error}")
          requeue_message(message)
      end
      drop_message(message)
  end

  defp drop_or_requeue_message(%Message{} = message, _state) do
    Logger.error("#{__MODULE__} expected [:drop, :keep, function/1] in on_failure")
    drop_message(message)
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

  defp find_producer_pid_from_opts(opts) do
      opts
      |> get_in([:opts, :queue_name])
      |> String.to_atom()
      |> Process.whereis()
  end

  defp add_default_options(opts) do
    [message_validators: [],
     bind_once: false,
     max_retries: 1,
     on_failure: :keep]
    |> Keyword.merge(opts)
    |> Enum.into(%{})
  end
end
