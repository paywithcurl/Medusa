alias Experimental.GenStage

defmodule Medusa.Consumer.RabbitMQ do
  use GenStage
  require Logger
  alias Medusa.Message

  defmodule State do
    defstruct callback: nil, producer: nil, opts: []
  end

  defmodule Failure do
    defstruct message: nil, reason: nil
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
    Logger.debug("Got unexpected message #{inspect msg} state #{inspect state} from #{inspect self()}")
    {:noreply, state}
  end

  def terminate(reason, state) do
    Logger.debug("""
      #{__MODULE__}
      state: #{inspect state}
      die: #{inspect reason}
    """)
  end

  defp do_handle_events([], state) do
    {:noreply, [], state}
  end

  defp do_handle_events(events, %{callback: f, opts: opts} = state) do
    Enum.each(events, fn message ->
      message_info = message.metadata["message_info"]
      with %AMQP.Channel{} <- message_info.channel,
           tag when is_number(tag) <- message_info.delivery_tag,
           validators = opts.message_validators,
           :ok <- Medusa.validate_message(validators, message)
      do
        do_event(message, f, message, state)
      else
        {:error, reason} ->
          opts.exception_hook.(message, reason)
          {:error, reason}
      end
    end)
    respone_handle_events(state)
  end

  defp scrub_message(%Message{metadata: metadata} = message) do
    new_metadata = Map.delete(metadata, "message_info")
    %{message | metadata: new_metadata}
  end

  defp do_event(%Message{} = message, [callback], original_message, %{opts: opts} = state) do
    message_to_send = scrub_message(message)
    try do
      {timer, result} = :timer.tc(fn -> callback.(message_to_send) end)
      case result do
        :ok ->
          total_timer = message.metadata["message_info"].processing_time + timer
          Medusa.Logger.info(original_message,
                             processing_time: total_timer,
                             belongs: "consumption")
          ack_message(original_message)
        :error ->
          opts.exception_hook.(message_to_send, "error processing message")
          retry_event(original_message, state)
        {:error, reason} ->
          opts.exception_hook.(message_to_send, inspect(reason))
          retry_event(original_message, state)
        error ->
          opts.exception_hook.(message_to_send, inspect(error))
          drop_message(original_message)
      end
    rescue
      error ->
        opts.exception_hook.(message_to_send, stack_error(error))
        retry_event(original_message, state)
    catch
      error ->
        opts.exception_hook.(message_to_send, stack_error(error))
        retry_event(original_message, state)
      error, _other_error ->
        opts.exception_hook.(message_to_send, stack_error(error))
        retry_event(original_message, state)
    end
  end

  defp do_event(%Message{} = message, [callback|tail], original_message, %{opts: opts} = state) do
    try do
      {timer, result} = :timer.tc(fn -> callback.(message) end)
      case result do
        %Message{} = new_message ->
          new_message = update_in(new_message.metadata["message_info"].processing_time, &(&1 + timer))
          do_event(new_message, tail, original_message, state)
        :error ->
          opts.exception_hook.(message, "error processing message")
          retry_event(original_message, state)
        {:error, reason} ->
          opts.exception_hook.(message, inspect(reason))
          retry_event(original_message, state)
        error ->
          opts.exception_hook.(message, inspect(error))
          failure = %Failure{message: original_message, reason: error}
          drop_or_requeue_message(failure, state)
      end
    rescue
      error ->
        opts.exception_hook(message, stack_error(error))
        retry_event(original_message, state)
    catch
      error ->
        opts.exception_hook(message, stack_error(error))
        retry_event(original_message, state)
      error, _other_error ->
        opts.exception_hook(message, stack_error(error))
        retry_event(original_message, state)
    end
  end

  defp retry_event(
      %Message{metadata: _metadata} = message,
      %State{opts: %{
        max_retries: max_retries,
        exception_hook: exception_hook}} = state
  ) do
    new_message = update_in(message.metadata["message_info"].retry, &(&1 + 1))
    retried = new_message.metadata["message_info"].retry
    if retried <= max_retries do
      time =
        Medusa.config()
        |> Keyword.get(:retry_consume_pow_base, 2)
        |> :math.pow(retried)
        |> round()
        |> :timer.seconds()
      Process.send_after(self(), {:retry, new_message}, time)
    else
      exception_hook.(message, "error processing message")
      failure = %Failure{message: new_message, reason: "reach max_retries"}
      drop_or_requeue_message(failure, state)
    end
  end

  defp respone_handle_events(%State{opts: %{bind_once: true}} = state) do
    {:stop, :normal, state}
  end

  defp respone_handle_events(%State{opts: %{bind_once: false}} = state) do
    {:noreply, [], state}
  end

  defp drop_or_requeue_message(
    %Failure{message: %Message{} = message},
    %{opts: %{on_failure: :drop}})
  do
    drop_message(message)
  end

  defp drop_or_requeue_message(
    %Failure{message: %Message{} = message},
    %{opts: %{on_failure: :keep}})
  do
    requeue_message(message)
  end

  defp drop_or_requeue_message(
    %Failure{
      message: %Message{} = message,
      reason: reason
    },
    %{opts: %{
      on_failure: callback,
      exception_hook: exception_hook
    }}) when is_function(callback, 2)
  do
    case callback.(message, reason) do
      :drop ->
        drop_message(message)
      :keep ->
        requeue_message(message)
      _error ->
        exception_hook.(message, "expect on_failure function to return [:drop, :keep]")
        requeue_message(message)
    end
    drop_message(message)
  end

  defp drop_or_requeue_message(
    %Failure{message: %Message{} = message}, %{opts: opts} = state)
  do
    opts.exception_hook.(message, "expect [:drop, :keep, fun/2] in on_failure")
    drop_message(message)
  end


  defp ack_message(
      %Message{metadata: %{
        "message_info" => %{
          channel: channel, delivery_tag: delivery_tag}}}) do
    AMQP.Basic.ack(channel, delivery_tag)
  end

  defp requeue_message(
      %Message{metadata: %{
        "message_info" => %{
          channel: channel, delivery_tag: delivery_tag}}} = message) do
    Logger.debug("Requeueing message #{inspect message}")
    AMQP.Basic.nack(channel, delivery_tag, requeue: true)
  end

  defp drop_message(
      %Message{metadata: %{
        "message_info" => %{
          channel: channel, delivery_tag: delivery_tag}}} = message) do
    Logger.debug("Dropping message #{inspect message}")
    AMQP.Basic.nack(channel, delivery_tag, requeue: false)
  end

  defp find_producer_pid_from_opts(opts) do
      opts
      |> get_in([:opts, :queue_name])
      |> String.to_atom()
      |> Process.whereis()
  end

  defp add_default_options(opts) do
    opts
    |> Keyword.put_new(:message_validators, [])
    |> Keyword.put_new(:bind_once, false)
    |> Keyword.put_new(:max_retries, 1)
    |> Keyword.put_new(:on_failure, :keep)
    |> Keyword.put_new(:exception_hook, default_exception_hook())
    |> Enum.into(%{})
  end

  defp default_exception_hook do
    medusa_logger = fn message, reason ->
      Medusa.Logger.error(message, reason: reason, belongs: "consumption")
    end
    Application.get_env(:medusa, :exception_hook, medusa_logger)
  end

  defp stack_error(error) do
    stack = :erlang.get_stacktrace()
    Exception.format(:error, [error | stack])
  end
end
