# TODO use standard filenames for tests
defmodule Medusa.Patch.Adapter.RabbitMQTest do
  use ExUnit.Case

  test "hello" do
    :ok = Medusa.consume("*", fn(x) ->
      IO.inspect("---------------")
      IO.inspect(x)
      IO.inspect("---------------")
      :ok
    end)
    :ok = Medusa.publish("foo", %{hi: 6})
    Application.get_env(:medusa, Medusa)
    |> IO.inspect
    :timer.sleep(10_000)

  end
end
