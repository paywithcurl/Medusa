defmodule Medusa.TestHelper do
  @moduledoc false

  def put_adapter_config(adapter) do
    import Supervisor.Spec, warn: false
    Application.put_env(:medusa, Medusa, [adapter: adapter])
    Supervisor.start_child(Medusa.Supervisor, worker(adapter, []))
  end

end
