defmodule Medusa.ProducerSupervisor do
  @moduledoc false
  use Supervisor

  def start_link() do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_args) do
    children = [
      worker(producer_module(), [], restart: :transient)
    ]

    supervise(children, strategy: :simple_one_for_one, max_restarts: 100, max_seconds: 5)
  end

  def start_child(topic, opts \\ []) do
    opts = Keyword.merge(opts, name: topic)
    Supervisor.start_child(__MODULE__, [opts])
  end

  def producer_module do
    [_, _, adapter] = Medusa.adapter |> Module.split
    ["Medusa", "Producer", adapter] |> Module.concat
  end

end
