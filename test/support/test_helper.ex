defmodule Medusa.TestHelper do
  @moduledoc false

  def restart_app do
    Application.stop(:medusa)
    Application.ensure_all_started(:medusa)
  end

  def put_adapter_config(adapter) do
    import Supervisor.Spec, warn: false
    opts = [
      adapter: adapter,
      group: "test-rabbitmq",
      retry_publish_backoff: 500,
      retry_publish_max: 1
    ]
    Application.put_env(:medusa, Medusa, opts, persistent: true)
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

  def error(_) do
    :error
  end

  def state(%{metadata: %{"agent" => agent, "times" => times} = metadata} = message) do
    val = agent |> String.to_atom |> Agent.get_and_update(&({&1, &1+1}))
    cond do
      metadata["bad_return"] ->
        :bad_return
      val == times ->
        :self |> Process.whereis |> send(message)
        :ok
      metadata["raise"] ->
        raise "Boom!"
      metadata["throw"] ->
        throw "Bamm!"
      true ->
        {:error, val}
    end
  end
end
