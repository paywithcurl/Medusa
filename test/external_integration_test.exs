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

  test "Add consumers" do
    assert {:ok, _pid} = Medusa.consume "foo.bob", &IO.puts/1
  end

  test "Send events" do
    Process.register self, :test
    Medusa.consume "foo.bar", &MyModule.echo/1
    Medusa.consume "foo.*", &MyModule.echo/1

    Medusa.publish "foo.bar", 90, %{"optional_field" => "nice_to_have", to: self}

    # We should receive two because of the routes setup.
    assert_receive %Message{body: 90, metadata: %{"optional_field" => "nice_to_have",
                                                  from: MyModule.Echo, to: _}}
    assert_receive %Message{body: 90, metadata: %{"optional_field" => "nice_to_have",
                                                  from: MyModule.Echo, to: _}}
  end

  test "Send event to consumer with bind_once: true.
        consumer and producer should die" do
    assert {:ok, pid} = Medusa.consume "you.me", &MyModule.echo/1, bind_once: true
    assert Process.alive?(pid)
    producer = Process.whereis(:"you.me")
    assert producer
    Medusa.publish "you.me", 100, %{to: self}
    assert_receive %Message{body: 100, metadata: %{to: _, from: MyModule.Echo}}
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
    producer = Process.whereis(:"me.you")
    assert producer
    Medusa.publish "me.you", 1000, %{to: self}
    assert_receive %Message{body: 1000, metadata: %{to: _self, from: MyModule.Echo}}
    assert_receive %Message{body: 1000, metadata: %{to: _self, from: MyModule.Ping}}
    Process.sleep(10) # wait consumer bind_once die
    assert Process.alive?(pid1)
    refute Process.alive?(pid2)
    assert Process.alive?(producer)
  end

end
