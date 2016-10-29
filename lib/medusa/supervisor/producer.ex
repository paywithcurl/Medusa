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

    supervise(children, strategy: :simple_one_for_one)
  end

  def start_child(topic) do
    Supervisor.start_child(__MODULE__, [[name: topic]])
  end

  def producer_module do
    [_, _, adapter] = Medusa.adapter |> Module.split
    ["Medusa", "Producer", adapter] |> Module.concat
  end

end
