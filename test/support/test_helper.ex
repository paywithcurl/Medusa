defmodule Medusa.TestHelper do
  @moduledoc false

  def restart_app do
    Application.stop(:medusa)
    Application.ensure_all_started(:medusa)
  end

  def put_adapter_config(adapter) do
    import Supervisor.Spec, warn: false
    Application.put_env(:medusa, Medusa, [adapter: adapter], persistent: true)
    restart_app()
  end

  def consumer_children do
    Supervisor.which_children(Medusa.ConsumerSupervisor)
  end

  def producer_children do
    Supervisor.which_children(Medusa.ProducerSupervisor)
  end

end

defmodule MyModule do
  def echo(message) do
    :self |> Process.whereis |> send(message)
  end
end
