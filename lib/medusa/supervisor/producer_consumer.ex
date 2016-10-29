defmodule Medusa.ProducerConsumerSupervisor do
  @moduledoc false
  use Supervisor

  def start_link() do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_args) do
    children = [
      supervisor(Medusa.ProducerSupervisor, []),
      supervisor(Medusa.ConsumerSupervisor, [])
    ]

    supervise(children, strategy: :one_for_all)
  end

end
