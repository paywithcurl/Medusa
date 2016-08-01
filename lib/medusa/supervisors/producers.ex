defmodule Medusa.Supervisors.Producers do
  @moduledoc false
  use Supervisor

  def start_link() do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_args) do
    children = [
      worker(Medusa.Producer, [], restart: :transient)
    ]

    supervise(children, strategy: :simple_one_for_one)
  end

  def start_child(regex) do
    Supervisor.start_child(__MODULE__, name: regex)
  end

end