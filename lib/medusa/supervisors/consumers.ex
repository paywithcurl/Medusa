defmodule Medusa.Supervisors.Consumers do
  @moduledoc false
  use Supervisor

  def start_link() do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_args) do
    children = [
      worker(Medusa.Consumer, [], restart: :transient)
    ]

    supervise(children, strategy: :simple_one_for_one)
  end

  def start_child(f, to_link, opts \\ []) do
    to_link = String.to_atom to_link
    Supervisor.start_child(__MODULE__, [[function: f, to_link: to_link, opts: opts]])
  end
end
