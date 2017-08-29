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
    {:ok, supervisor} = Supervisor.start_link([],
                                              strategy: :one_for_one,
                                              max_restarts: 100,
                                              max_seconds: 5,
                                              name: Medusa.Supervisor)

    # MedusaConfig needs to be started before child_adapter is called
    {:ok, _} = Supervisor.start_child(supervisor, config_worker())
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
  def consume(route, functions, opts \\ []) do
    case validate_consume_function(functions) do
      :ok ->
        adapter().new_route(route, functions, opts)
      {:error, reason} ->
        Logger.error(reason)
        {:error, reason}
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
    message = %Message{topic: event, body: payload, metadata: metadata}
    validators = Keyword.get(opts, :message_validators, [])
    case validate_message(validators, message) do
      :ok ->
        adapter().publish(message)
      {:error, reason} ->
        Medusa.Logger.error(message,
                            reason: "publish failed: #{reason}",
                            belongs: "publishing")
        {:error, "message is invalid"}
    end
  end

  def alive? do
    adapter().alive?
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
    functions = List.wrap(functions)
    case MedusaConfig.get_message_validator(:medusa_config) do
      fun when is_function(fun, 1) -> [fun | functions]
      nil -> functions
      _others -> {:error, "validate_message must be a function with arity 1"}
    end
    |> do_validate_message(message)
  end

  defp child_adapter do
    adapter()
    |> worker([])
  end

  defp child_queue do
    case adapter() do
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
    app_config = config() || []
    adapter = Keyword.get(app_config, :adapter)
    cond do
      adapter in @available_adapters ->
        :ok
      true ->
        new_app_config = Keyword.merge(app_config, [adapter: @default_adapter])
        Application.put_env(:medusa, Medusa, new_app_config, persistent: true)
    end
  end

  defp validate_consume_function(function) when is_function(function, 1) do
    validate_consume_function([function])
  end

  defp validate_consume_function([]) do
    :ok
  end

  defp validate_consume_function([function|tail]) when is_function(function, 1) do
    validate_consume_function(tail)
  end

  defp validate_consume_function(_) do
    {:error, "consume must be function with arity 1"}
  end

  defp do_validate_message({:error, reason}, _message) do
    {:error, reason}
  end

  defp do_validate_message([], _message) do
    :ok
  end

  defp do_validate_message([function|tail], %Message{} = message)
      when is_function(function, 1) do
    case apply(function, [message]) do
      :ok ->
        do_validate_message(tail, message)
      {:error, reason} ->
        {:error, reason}
      reason ->
        {:error, "invalid return from validator, expected [:ok, {:error, reason}]"}
    end
  end

  defp do_validate_message(_, %Message{} = message) do
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
