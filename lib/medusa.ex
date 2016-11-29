defmodule Medusa do
  use Application
  require Logger
  import Supervisor.Spec, warn: false

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

  def consume(route, functions, opts \\ []) do
    functions
    |> List.wrap
    |> Enum.all?(&(:erlang.fun_info(&1, :arity)) |> elem(1) == 1)
    |> case do
      true ->
        Medusa.Broker.new_route(route, functions, opts)
      false ->
        Logger.warn("consume function must have arity 1")
        {:error, "arity must be 1"}
    end
  end

  def publish(event, payload, metadata \\ %{}) do
    metadata = cond do
      Map.has_key?(metadata, :id) -> metadata
      true -> Map.put(metadata, :id, UUID.uuid4)
    end

    case is_message_valid?(event, payload, metadata) do
      true -> Medusa.Broker.publish(event, payload, metadata)
      false ->
        Logger.warn "Message failed validation #{event} #{inspect payload} #{inspect metadata}"
        {:error, "message is invalid"}
    end
  end

  def adapter do
    MedusaConfig.get_adapter(:medusa_config)
  end

  def config, do: Application.get_env(:medusa, Medusa)

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

  defp is_message_valid?(event, payload, metadata) do
    case MedusaConfig.get_message_validator(:medusa_config) do
      nil -> true
      f -> f.(event, payload, metadata)
    end
  end

end
