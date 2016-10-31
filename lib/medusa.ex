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
    children =
      [
        child_adapter(),
        child_queue(),
        supervisor(Task.Supervisor, [[name: Broker.Supervisor]]),
        supervisor(Medusa.ProducerConsumerSupervisor, [])
      ]
      |> List.flatten

    opts = [strategy: :one_for_one, name: Medusa.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def consume(route, function, opts \\ []) do
    {_, 1} =  :erlang.fun_info(function, :arity)
    Medusa.Broker.new_route(route, function, opts)
  end

  def publish(event, payload, metadata \\ %{}) do
    Medusa.Broker.publish event, payload, metadata
  end

  def adapter do
    :medusa
    |> Application.get_env(Medusa)
    |> Keyword.fetch!(:adapter)
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

end
