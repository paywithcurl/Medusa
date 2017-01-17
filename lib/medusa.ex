defmodule Medusa do
  use Application
  require Logger
  import Supervisor.Spec, warn: false
  alias Medusa.Message

  @available_adapters [Medusa.Adapter.PG2,
                       Medusa.Adapter.RabbitMQ]
  @default_adapter Medusa.Adapter.PG2

  @moduledoc """
  Medusa is a Pub/Sub system that leverages GenStage.

  You should declare routes in `String` like
  the following examples:

  ```
  Medusa.consume "foo.bar", &Honey.doo/1   # Matches only "foo.bar" events.
  Medusa.consume "foo.*" &Lmbd.bo/1        # Matches all "foo. ..." events
  ```

  Then, to publish something, you call:

  ```
  Medusa.publish "foo.bar", my_awesome_payload
  ```

  ## Caveats

  It can only consume functions of arity 1.

  """

  def start(_type, _args) do
    ensure_config_correct()
    {:ok, supervisor} = Supervisor.start_link([], [strategy: :one_for_one, name: Medusa.Supervisor])

    # MedusaConfig needs to be started before child_adapter is called
    {:ok, _} = Supervisor.start_child(supervisor, config_worker)
    children =
      [
        child_adapter(),
        child_queue(),
        supervisor(Task.Supervisor, [[name: Broker.Supervisor]]),
        supervisor(Medusa.ProducerConsumerSupervisor, [])
      ]
      |> List.flatten

    Enum.each children, fn (child) -> Supervisor.start_child(supervisor, child) end
    {:ok, supervisor}
  end

  @doc """
  Adds a new route using the configured adapter.
  """
  def consume(route, action, opts \\ [])
  def consume(route, action, opts) when is_function(action, 1) do
    callback = fn(message) ->
      case action.(message) do
        # TODO make this more specific {:ok, message} {:error, reason}
        :ok ->
          Logger.info("message consumed")
          Logger.info(message |> Map.delete(:body) |> inspect)
          Logger.debug(message.body |> inspect)
          :ok
        other ->
          Logger.error("message not published #{other |> inspect}")
          Logger.error(message |> inspect)
          other
      end
    end
    adapter().new_route(route, callback, opts)
  end
  def consume(route, functions, opts) when is_list(functions) do
    case compose(functions) do
      {:ok, function} ->
        adapter().new_route(route, function, opts)
      {:error, :invalid_function} ->
        Logger.warn("consume must be function")
        {:error, :invalid_function}
    end
  end

  def compose([f]) do
    {:ok, f}
  end
  def compose([a, b | rest]) do
    if is_function(a, 1) and is_function(b, 1) do
      f = fn(x) ->
        b.(a.(x))
      end
      compose([f | rest])
    else
      {:error, :invalid_function}
    end
  end

  @doc """
  Sends to the matching routes the event, using the configured adapter.
  metadata keys will always convert to string
  """
  def publish(event, payload, metadata \\ %{}, opts \\ []) do
    metadata =
      metadata
      |> map_key_to_string
      |> Map.merge(%{"id" => UUID.uuid4}, fn _k, v1, _v2 -> v1 end)
      |> Map.put("event", event)
    message = %Message{topic: event, body: payload, metadata: metadata}
    opts
    |> Keyword.get(:message_validators, [])
    |> validate_message(message)
    |> case do
      :ok ->
        case adapter().publish(message) do
          :ok ->
            Logger.info("message published")
            Logger.info(message |> Map.delete(:body) |> inspect)
            # Body should always come in the same format.
            Logger.debug(message.body |> inspect)
            :ok
          other ->
            Logger.error("message not published #{other |> inspect}")
            Logger.error(message |> inspect)
            other
        end

      {:error, reason} ->
        Logger.warn "Message failed validation #{inspect reason}: #{event} #{inspect payload} #{inspect metadata}"
        {:error, "message is invalid"}
    end
  end

  def alive? do
    adapter.alive?
  end

  def adapter do
    MedusaConfig.get_adapter(:medusa_config)
  end

  def config, do: Application.get_env(:medusa, Medusa)

  @doc """
  Validate message againts list of functions.
  function must be arity/3 (event, payload, metadata).
  return :ok if valid and {:error, reason} if invalid.
  validate_message always execute global_validator first if provided.
  global_validator set by

      config :medusa, Medusa,
        validate_message: &function/3
  """
  def validate_message(functions, %Message{} = message) do
    global_validator = MedusaConfig.get_message_validator(:medusa_config)
    functions =
      cond do
        is_function(global_validator) -> [global_validator|List.wrap(functions)]
        true -> List.wrap(functions)
      end
    do_validate_message(functions, message)
  end

  defp child_adapter do
    adapter
    |> worker([])
  end

  defp child_queue do
    case adapter do
      Medusa.Adapter.PG2 -> worker(Medusa.Queue, [])
      _ -> []
    end
  end

  defp config_worker do
    env = Application.get_env(:medusa, Medusa)
    worker(MedusaConfig, [%{
      adapter: env[:adapter],
      message_validator: env[:message_validator]
    }])
  end

  defp ensure_config_correct do
    app_config = Application.get_env(:medusa, Medusa, [])
    adapter = Keyword.get(app_config, :adapter)
    cond do
      adapter in @available_adapters ->
        :ok
      true ->
        new_app_config = Keyword.merge(app_config, [adapter: @default_adapter])
        Application.put_env(:medusa, Medusa, new_app_config, persistent: true)
    end
  end

  defp do_validate_message([], _message) do
    :ok
  end

  defp do_validate_message([function|tail], %Message{} = message)
  when is_function(function) do
    case apply(function, [message]) do
      :ok -> do_validate_message(tail, message)
      {:error, reason} -> {:error, reason}
      reason -> {:error, reason}
    end
  end

  defp do_validate_message(_, _) do
    {:error, "validator is not a function"}
  end

  defp map_key_to_string(%{} = map) do
    Enum.reduce(map, %{}, fn
      {key, %{} = val}, acc ->
        Map.put(acc, to_string(key), map_key_to_string(val))
      {key, val}, acc ->
        Map.put(acc, to_string(key), val)
    end)
  end

end
