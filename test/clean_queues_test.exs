defmodule CleanQueue do
  use ExUnit.Case

  setup_all do
    Medusa.TestHelper.put_rabbitmq_adapter_config()
    on_exit fn ->
      Medusa.TestHelper.delete_all_queues()
    end
  end

  test "clean queues" do
  end
end
