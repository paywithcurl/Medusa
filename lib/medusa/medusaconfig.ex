defmodule MedusaConfig do
  use GenServer

  def start_link(state) do
    GenServer.start_link(__MODULE__, state, name: :medusa_config)
  end

  def get_adapter(pid) do
    GenServer.call(pid, :get_adapter)
  end

  def get_message_validator(pid) do
    GenServer.call(pid, :get_message_validator)
  end

  def set_message_validator(pid, message_validator) do
    GenServer.call(pid, {:set_message_validator, message_validator})
  end

  def handle_call(:get_adapter, _from, state) do
    {:reply, state[:adapter], state}
  end

  def handle_call(:get_message_validator, _from, state) do
    {:reply, state[:message_validator], state}
  end

  def handle_call({:set_message_validator, message_validator}, _from, state) do
    {:reply, :ok, Map.put(state, :message_validator, message_validator)}
  end

end
