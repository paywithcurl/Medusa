defmodule Medusa do
  use Application
  require Logger
  import Supervisor.Spec, warn: false

  @available_adapters ~w(Medusa.Adapter.Local)
  @default_adapter Medusa.Adapter.Local

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
    children = [
      worker(Medusa.Broker, []),
      child_adapter(),
      supervisor(Task.Supervisor, [[name: Broker.Supervisor]]),
      supervisor(Medusa.Supervisors.Producers, []),
      supervisor(Medusa.Supervisors.Consumers, [])
    ]

    opts = [strategy: :one_for_one, name: Medusa.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def consume(route, function, opts \\ []) do
    {_, 1} =  :erlang.fun_info(function, :arity)

    # Register an route on the Broker
    Medusa.Broker.new_route(route)

    Medusa.Supervisors.Producers.start_child(route)
    Medusa.Supervisors.Consumers.start_child(function, route, opts)
  end

  def publish(event, payload, metadata \\ %{}) do
    Medusa.Broker.publish event, payload, metadata
  end

  defp child_adapter do
    :medusa
    |> Application.get_env(Medusa)
    |> Keyword.fetch!(:adapter)
    |> worker([])
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
