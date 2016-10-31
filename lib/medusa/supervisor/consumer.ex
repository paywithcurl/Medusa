defmodule Medusa.ConsumerSupervisor do
  @moduledoc false
  use Supervisor

  def start_link() do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_args) do
    children = [
      worker(consumer_module(), [], restart: :transient)
    ]

    supervise(children, strategy: :simple_one_for_one)
  end

  def start_child(f, to_link, opts \\ []) do
    to_link =
      cond do
        is_binary(to_link) -> String.to_atom(to_link)
        is_pid(to_link) -> to_link
        is_atom(to_link) -> to_link
      end
    Supervisor.start_child(__MODULE__, [[function: f, to_link: to_link, opts: opts]])
  end

  defp consumer_module do
    [_, _, adapter] = Medusa.adapter |> Module.split
    ["Medusa", "Consumer", adapter] |> Module.concat
  end

end
