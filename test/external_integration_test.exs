defmodule MyModule do
  def echo(message) do
    message = put_in message, [Access.key(:metadata), :from], MyModule.Echo
    send message.metadata.to, message
  end

  def ping(message) do
    message = put_in message, [Access.key(:metadata), :from], MyModule.Ping
    send message.metadata.to, message
  end
end

defmodule ExternalIntegrationTest do
  use ExUnit.Case
  require Medusa

  alias Medusa.Broker.Message

  setup do
    fc = fn x -> send(:test, {:hey, "You sent me", x}) end
    [fc: fc]
  end

  test "Add consumers", ctx do
    assert {:ok, _pid} = Medusa.consume "foo.bob", ctx[:fc]
  end

  test "Add invalid consumer", ctx do
    assert_raise MatchError, ~r/arity/, fn -> Medusa.consume "foo.bob", fn -> IO.puts("blah") end end
  end


  test "Send events", ctx do
    Process.register self, :test
    Medusa.consume "foo.bar", ctx[:fc]
    Medusa.consume "foo.*", ctx[:fc]

    Medusa.publish "foo.bar", 90, %{"optional_field" => "nice_to_have"}

    # We should receive two because of the routes setup.
    assert_receive {:hey, "You sent me", %Message{body: 90, metadata: %{"optional_field" => "nice_to_have"}}}
    assert_receive {:hey, "You sent me", %Message{body: 90, metadata: %{"optional_field" => "nice_to_have"}}}
  end

  test "Send event to consumer with bind_once: true.
        consumer and producer should die" do
    assert {:ok, pid} = Medusa.consume "you.me", &MyModule.echo/1, bind_once: true
    assert Process.alive?(pid)
    assert producer = Process.whereis(:"you.me")
    Medusa.publish "you.me", 100, %{to: self}
    assert_receive %Message{body: 100, metadata: %{to: self, from: MyModule.Echo}}
    Process.sleep(10) # wait producer and consumer die
    refute Process.alive?(pid)
    refute Process.alive?(producer)
  end

  test "Send event to consumer with bind_once: true.
        But route is shared with others then producer should not die" do
    assert {:ok, pid1} = Medusa.consume "me.you", &MyModule.ping/1
    assert {:ok, pid2} = Medusa.consume "me.you", &MyModule.echo/1, bind_once: true
    assert Process.alive?(pid1)
    assert Process.alive?(pid2)
    assert producer = Process.whereis(:"me.you")
    Medusa.publish "me.you", 1000, %{to: self}
    assert_receive %Message{body: 1000, metadata: %{to: self, from: MyModule.Echo}}
    assert_receive %Message{body: 1000, metadata: %{to: self, from: MyModule.Ping}}
    Process.sleep(10) # wait consumer bind_once die
    assert Process.alive?(pid1)
    refute Process.alive?(pid2)
    assert Process.alive?(producer)
  end

end
