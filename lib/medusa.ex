defmodule Medusa do
  use Application
  require Logger

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

  defmacro consume(event, module, function) do
    quote do
      # Register an route on the Broker
      Medusa.Broker.new_route(unquote(event))
      f = fn x -> unquote(module).unquote(function)(x) end

      # Can't use PID here, because we need to register by name.
      case Medusa.Supervisors.Producers.start_child(unquote(event)) do
        {:ok, _pid} ->
          Medusa.Supervisors.Consumers.start_child(f, unquote(event))
        {:error, {:already_started, _pid}} ->
          Medusa.Supervisors.Consumers.start_child(f, unquote(event))
        bleh ->
          raise "Shit happened! #{bleh}"
      end
    end
  end
end
