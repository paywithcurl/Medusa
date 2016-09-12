defmodule ExternalIntegrationTest do
  use ExUnit.Case
  require Medusa

  setup do
    fc = fn x -> send(:test, {:hey, "You sent me", x}) end
    [fc: fc]
  end

  test "Add consumers", ctx do
    assert {:ok, _pid} = Medusa.consume ~r/^foo\.bob$/, ctx[:fc]
  end

  test "Send events", ctx do
    Process.register self, :test
    Medusa.consume ~r/^foo\.bar$/, ctx[:fc]
    Medusa.consume ~r/^foo\.*/, ctx[:fc]

    Medusa.publish "foo.bar", 90, %{"optional_field" => "nice_to_have"}

    # We should receive two because of the routes setup.
    assert_receive {:hey, "You sent me", %Medusa.Broker.Message{body: 90, metadata: %{"optional_field" => "nice_to_have"}}}
    assert_receive {:hey, "You sent me", %Medusa.Broker.Message{body: 90, metadata: %{"optional_field" => "nice_to_have"}}}
  end
  
end
