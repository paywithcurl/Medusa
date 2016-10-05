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

  test "Add invalid consumer" do
    assert_raise MatchError, ~r/arity/, fn -> Medusa.consume "foo.bob", fn -> IO.puts("blah") end end
  end

  test "Send events" do
    Process.register self, :test
    Medusa.consume "foo.bar", &MyModule.echo/1
    Medusa.consume "foo.*", &MyModule.echo/1

    Medusa.publish "foo.bar", 90, %{"optional_field" => "nice_to_have", to: self}

    # We should receive two because of the routes setup.
    assert_receive %Message{body: 90, metadata: %{"optional_field" => "nice_to_have", from: MyModule.Echo, to: _self}}, 500
    assert_receive %Message{body: 90, metadata: %{"optional_field" => "nice_to_have", from: MyModule.Echo, to: _self}}, 500
  end

  test "Send event to consumer with bind_once: true.
        consumer and producer should die" do
    assert {:ok, pid} = Medusa.consume "you.me", &MyModule.echo/1, bind_once: true
    producer = Process.whereis(:"you.me")
    assert Process.alive?(pid)
    assert producer
    Medusa.publish "you.me", 100, %{to: self}
    assert_receive %Message{body: 100, metadata: %{to: _self, from: MyModule.Echo}}, 500
    Process.sleep(10) # wait producer and consumer die
    refute Process.alive?(pid)
    refute Process.alive?(producer)
  end

  test "Send event to consumer with bind_once: true in already exists route
        So producer is shared with others then it should not die" do
    assert {:ok, pid1} = Medusa.consume "me.you", &MyModule.ping/1
    assert {:ok, pid2} = Medusa.consume "me.you", &MyModule.echo/1, bind_once: true
    producer = Process.whereis(:"me.you")
    assert Process.alive?(pid1)
    assert Process.alive?(pid2)
    assert producer
    Medusa.publish "me.you", 1000, %{to: self}
    assert_receive %Message{body: 1000, metadata: %{to: _self, from: MyModule.Echo}}, 500
    assert_receive %Message{body: 1000, metadata: %{to: _self, from: MyModule.Ping}}, 500
    Process.sleep(10) # wait consumer bind_once die
    assert Process.alive?(pid1)
    refute Process.alive?(pid2)
    assert Process.alive?(producer)
  end

  test "Send event to consumer with bind_once: true and then
        start consume with long-running consumer, producer should survive" do
    assert {:ok, pid1} = Medusa.consume "he.she", &MyModule.echo/1, bind_once: true
    assert {:ok, pid2} = Medusa.consume "he.she", &MyModule.ping/1
    assert Process.alive?(pid1)
    assert Process.alive?(pid2)
    producer = Process.whereis(:"he.she")
    Medusa.publish "he.she", 1, %{to: self}
    assert_receive %Message{body: 1, metadata: %{to: _self, from: MyModule.Echo}}, 500
    assert_receive %Message{body: 1, metadata: %{to: _self, from: MyModule.Ping}}, 500
    Process.sleep(10) # wait consumer bind_once die
    refute Process.alive?(pid1)
    assert Process.alive?(pid2)
    assert Process.alive?(producer)
  end

  @tag :clustered
  test "clustered" do
    node1 = :'node1@127.0.0.1'
    node2 = :'node2@127.0.0.1'
    Medusa.consume "clustered", &MyModule.echo/1
    :rpc.call(node1, Medusa, :publish, ["clustered", "ICANFLY", %{to: self}])
    :rpc.call(node2, Medusa, :publish, ["clustered", "YOUSEEME", %{to: self}])
    assert_receive %Message{body: "ICANFLY", metadata: %{to: _, from: MyModule.Echo}}, 500
    assert_receive %Message{body: "YOUSEEME", metadata: %{to: _, from: MyModule.Echo}}, 500
  end

end
