defmodule MedusaConfig do
  use GenServer

  def start_link(state, opts \\ []) do
    GenServer.start_link(__MODULE__, state, opts)
  end

  def get_adapter(pid) do
    GenServer.call(pid, :get_adapter)
  end

  def handle_call(:get_adapter, _from, state) do
    {:reply, state[:adapter], state}
  end

end
