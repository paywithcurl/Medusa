defmodule Medusa do
  use Application
  require Logger

  @moduledoc """
  Medusa is a Pub/Sub system that leverages GenStage.

  You should declare routes using `Regex` module, like
  the following examples:

  ```
  Medusa.consume ~r/^foo\.bar$/, &Honey.doo/1   # Matches only "foo.bar" events.
  Medusa.consume ~r/^foo\.*/, &Lmbd.bo/1        # Matches all "foo. ..." events.
  ```

  Then, to publish something, you call:

  ```
  Medusa.publish "foo.bar", my_awesome_payload
  ```

  ## Caveats

  It can only consume functions of arity 1.

  """

  @misconfiguration_error """
    Oops... looks like Medusa is not configured.
    Please, check if you have a line like this in your configuration:

    config :medusa, Medusa,
            adapter: Medusa.Adapter.Local

    Medusa has support for other adapers. Check them in Hex.
    Don't worry, she will not turn you into stone... yet.
  """


  # Check if configuration exists.
  unless Application.get_env(:medusa, Medusa) do
    raise @misconfiguration_error
  end

  unless Keyword.get(Application.get_env(:medusa, Medusa), :adapter) do
    raise @misconfiguration_error
  end


  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    children = [
      worker(Medusa.Broker, []),
      supervisor(Medusa.Supervisors.Producers, []),
      supervisor(Medusa.Supervisors.Consumers, [])
    ] |> start_local_adapter_if_configured

    opts = [strategy: :one_for_one, name: Medusa.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def start_local_adapter_if_configured(children) do
    import Supervisor.Spec, warn: false

    if Keyword.get(Application.get_env(:medusa, Medusa), :adapter) == Medusa.Adapter.Local do
      [worker(Medusa.Adapter.Local, []) | children]
    end
  end

  defmacro consume(event, function) do
    quote do
      # Register an route on the Broker
      Medusa.Broker.new_route(unquote(event))

      # Can't use PID here, because we need to register by name.
      case Medusa.Supervisors.Producers.start_child(unquote(event)) do
        {:ok, _pid} ->
          Medusa.Supervisors.Consumers.start_child(unquote(function), unquote(event))
        {:error, {:already_started, _pid}} ->
          Medusa.Supervisors.Consumers.start_child(unquote(function), unquote(event))
        bleh ->
          raise "Shit happened! #{bleh}"
      end
    end
  end

  def publish(route, payload) do
    Medusa.Broker.publish route, payload
  end
end
