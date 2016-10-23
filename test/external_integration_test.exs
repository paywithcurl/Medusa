defmodule MyModule do
  def echo(message) do
    message = put_in message, [Access.key(:metadata), :from], MyModule.Echo
    send(message.metadata.to, message)
  end

  def ping(message) do
    message = put_in message, [Access.key(:metadata), :from], MyModule.Ping
    send(message.metadata.to, message)
  end

  def rpc(message) do
    message = put_in message, [Access.key(:metadata), :from], MyModule.RPC
    :self |> Process.whereis |> send(message)
  end
end

defmodule ExternalIntegrationTest do
  use ExUnit.Case
  require Medusa
  import Medusa.TestHelper
  alias Medusa.Broker.Message

  describe "Local Adapter" do

    setup do
      put_adapter_config(Medusa.Adapter.Local)
      :ok
    end

    test "Send events" do
      Medusa.consume("foo.bar", &MyModule.echo/1)
      Medusa.consume("foo.*", &MyModule.ping/1)
      Medusa.publish("foo.bar", 90, %{"optional_field" => "nice_to_have", to: self})
      assert_receive %Message{body: 90, metadata: %{"optional_field" => "nice_to_have"}}
      assert_receive %Message{body: 90, metadata: %{"optional_field" => "nice_to_have"}}
    end

    test "Send event to consumer with bind_once: true.
          consumer and producer should die" do
      assert {:ok, consumer} = Medusa.consume("you.me", &MyModule.echo/1, bind_once: true)
      assert Process.alive?(consumer)
      producer = Process.whereis(:"you.me")
      assert Process.alive?(producer)
      ref_consumer = Process.monitor(consumer)
      ref_producer = Process.monitor(producer)
      Medusa.publish("you.me", 100, %{to: self})
      assert_receive %Message{body: 100, metadata: %{to: _self, from: MyModule.Echo}}, 500
      assert_receive {:DOWN, ^ref_consumer, :process, _, :normal}
      assert_receive {:DOWN, ^ref_producer, :process, _, :normal}
    end

    test "Send event to consumer with bind_once: true in already exists route
          So producer is shared with others then it should not die" do
      assert {:ok, con1} = Medusa.consume("me.you", &MyModule.ping/1)
      assert {:ok, con2} = Medusa.consume("me.you", &MyModule.echo/1, bind_once: true)
      assert Process.alive?(con1)
      assert Process.alive?(con2)
      producer = Process.whereis(:"me.you")
      assert Process.alive?(producer)
      ref_con1 = Process.monitor(con1)
      ref_con2 = Process.monitor(con2)
      ref_prod = Process.monitor(producer)
      Medusa.publish("me.you", 1000, %{to: self})
      assert_receive %Message{body: 1000, metadata: %{to: _self, from: MyModule.Echo}}, 500
      assert_receive %Message{body: 1000, metadata: %{to: _self, from: MyModule.Ping}}, 500
      refute_receive {:DOWN, ^ref_con1, :process, _, :normal}
      assert_receive {:DOWN, ^ref_con2, :process, _, :normal}
      refute_receive {:DOWN, ^ref_prod, :process, _, :normal}
    end

    test "Send event to consumer with bind_once: true and then
          start consume with long-running consumer, producer should survive" do
      assert {:ok, con1} = Medusa.consume("he.she", &MyModule.echo/1, bind_once: true)
      assert {:ok, con2} = Medusa.consume("he.she", &MyModule.ping/1)
      assert Process.alive?(con1)
      assert Process.alive?(con2)
      producer = Process.whereis(:"he.she")
      assert Process.alive?(producer)
      ref_con1 = Process.monitor(con1)
      ref_con2 = Process.monitor(con2)
      ref_prod = Process.monitor(producer)
      Medusa.publish("he.she", 1, %{to: self})
      assert_receive %Message{body: 1, metadata: %{to: _self, from: MyModule.Echo}}, 500
      assert_receive %Message{body: 1, metadata: %{to: _self, from: MyModule.Ping}}, 500
      assert_receive {:DOWN, ^ref_con1, :process, _, :normal}
      refute_receive {:DOWN, ^ref_con2, :process, _, :normal}
      refute_receive {:DOWN, ^ref_prod, :process, _, :normal}
    end

  end


  describe "PG2 Adapter" do
    setup do
      put_adapter_config(Medusa.Adapter.PG2)
      Medusa.Cluster.spawn
      {:ok, node1: :'node1@127.0.0.1', node2: :'node2@127.0.0.1'}
    end

    @tag :pg2
    test "Sent events", %{node1: node1, node2: node2} do
      Medusa.consume("pg", &MyModule.echo/1)
      :rpc.call(node1, Medusa, :publish, ["pg", "ICANFLY", %{to: self}])
      :rpc.call(node2, Medusa, :publish, ["pg", "YOUSEEME", %{to: self}])
      assert_receive %Message{body: "ICANFLY", metadata: %{to: _, from: MyModule.Echo}}, 500
      assert_receive %Message{body: "YOUSEEME", metadata: %{to: _, from: MyModule.Echo}}, 500
    end

    @tag :pg2
    test "Send event to consumer with bind_once: true.
          consumer and producer should die", %{node1: node1} do
      assert {:ok, consumer} = Medusa.consume("pg.1", &MyModule.echo/1, bind_once: true)
      assert Process.alive?(consumer)
      producer = Process.whereis(:"pg.1")
      assert Process.alive?(producer)
      ref_consumer = Process.monitor(consumer)
      ref_producer = Process.monitor(producer)
      :rpc.call(node1, Medusa, :publish, ["pg.1", "HOLA", %{to: self}])
      assert_receive %Message{body: "HOLA", metadata: %{to: _, from: MyModule.Echo}}, 500
      assert_receive {:DOWN, ^ref_consumer, :process, _, :normal}
      assert_receive {:DOWN, ^ref_producer, :process, _, :normal}
    end

    @tag :pg2
    test "Send event to consumer with bind_once: true in already exists route
          So producer is shared with others then it should not die", %{node1: node1} do
      assert {:ok, con1} = Medusa.consume("pg.2", &MyModule.ping/1)
      assert {:ok, con2} = Medusa.consume("pg.2", &MyModule.echo/1, bind_once: true)
      assert Process.alive?(con1)
      assert Process.alive?(con2)
      producer = Process.whereis(:"pg.2")
      assert Process.alive?(producer)
      ref_con1 = Process.monitor(con1)
      ref_con2 = Process.monitor(con2)
      ref_prod = Process.monitor(producer)
      :rpc.call(node1, Medusa, :publish, ["pg.2", "GRACIAS", %{to: self}])
      assert_receive %Message{body: "GRACIAS", metadata: %{to: _self, from: MyModule.Echo}}, 500
      assert_receive %Message{body: "GRACIAS", metadata: %{to: _self, from: MyModule.Ping}}, 500
      refute_receive {:DOWN, ^ref_con1, :process, _, :normal}
      assert_receive {:DOWN, ^ref_con2, :process, _, :normal}
      refute_receive {:DOWN, ^ref_prod, :process, _, :normal}
    end

    @tag :pg2
    test "Send event to consumer with bind_once: true and then
          start consume with long-running consumer, producer should survive", %{node1: node1} do
      assert {:ok, con1} = Medusa.consume("pg.3", &MyModule.echo/1, bind_once: true)
      assert {:ok, con2} = Medusa.consume("pg.3", &MyModule.ping/1)
      assert Process.alive?(con1)
      assert Process.alive?(con2)
      producer = Process.whereis(:"pg.3")
      assert Process.alive?(producer)
      ref_con1 = Process.monitor(con1)
      ref_con2 = Process.monitor(con2)
      ref_prod = Process.monitor(producer)
      :rpc.call(node1, Medusa, :publish, ["pg.3", "ADIOS", %{to: self}])
      assert_receive %Message{body: "ADIOS", metadata: %{to: _self, from: MyModule.Echo}}, 500
      assert_receive %Message{body: "ADIOS", metadata: %{to: _self, from: MyModule.Ping}}, 500
      assert_receive {:DOWN, ^ref_con1, :process, _, :normal}
      refute_receive {:DOWN, ^ref_con2, :process, _, :normal}
      refute_receive {:DOWN, ^ref_prod, :process, _, :normal}
    end
  end

  describe "RabbitMQ Adapter" do
    setup do
      put_adapter_config(Medusa.Adapter.RabbitMQ)
      Process.register(self, :self)
      :ok
    end

    @tag :rabbitmq
    test "sent events" do
      Medusa.consume("rabbitmq", &MyModule.rpc/1)
      spawn fn -> Medusa.publish("rabbitmq", "RABBIT!") end
      assert_receive %Message{body: "RABBIT!", metadata: %{from: MyModule.RPC}}, 500
    end

    @tag :rabbitmq
    test "Send event to consumer with bind_once: true.
          consumer and producer should die" do
      assert {:ok, consumer} = Medusa.consume("rabbitmq.1", &MyModule.rpc/1, bind_once: true)
      assert Process.alive?(consumer)
      producer = Process.whereis(:"rabbitmq.1")
      assert Process.alive?(producer)
      ref_consumer = Process.monitor(consumer)
      ref_producer = Process.monitor(producer)
      spawn fn -> Medusa.publish("rabbitmq.1", "HOLA") end
      assert_receive %Message{body: "HOLA", metadata: %{from: MyModule.RPC}}, 500
      assert_receive {:DOWN, ^ref_consumer, :process, _, :normal}
      assert_receive {:DOWN, ^ref_producer, :process, _, :normal}
    end

    @tag :rabbitmq
    test "Send event to consumer with bind_once: true in already exists route
          So producer is shared with others then it should not die" do
      assert {:ok, con1} = Medusa.consume("rabbitmq.2", &MyModule.rpc/1)
      assert {:ok, con2} = Medusa.consume("rabbitmq.2", &MyModule.rpc/1, bind_once: true)
      assert Process.alive?(con1)
      assert Process.alive?(con2)
      producer = Process.whereis(:"rabbitmq.2")
      assert Process.alive?(producer)
      ref_con1 = Process.monitor(con1)
      ref_con2 = Process.monitor(con2)
      ref_prod = Process.monitor(producer)
      spawn fn -> Medusa.publish("rabbitmq.2", "GRACIAS") end
      assert_receive %Message{body: "GRACIAS", metadata: %{from: MyModule.RPC}}, 500
      assert_receive %Message{body: "GRACIAS", metadata: %{from: MyModule.RPC}}, 500
      refute_receive {:DOWN, ^ref_con1, :process, _, :normal}
      assert_receive {:DOWN, ^ref_con2, :process, _, :normal}
      refute_receive {:DOWN, ^ref_prod, :process, _, :normal}
    end

    @tag :rabbitmq
    test "Send event to consumer with bind_once: true and then
          start consume with long-running consumer, producer should survive" do
      assert {:ok, con1} = Medusa.consume("rabbitmq.3", &MyModule.rpc/1, bind_once: true)
      assert {:ok, con2} = Medusa.consume("rabbitmq.3", &MyModule.rpc/1)
      assert Process.alive?(con1)
      assert Process.alive?(con2)
      producer = Process.whereis(:"rabbitmq.3")
      assert Process.alive?(producer)
      ref_con1 = Process.monitor(con1)
      ref_con2 = Process.monitor(con2)
      ref_prod = Process.monitor(producer)
      spawn fn -> Medusa.publish("rabbitmq.3", "ADIOS") end
      assert_receive %Message{body: "ADIOS", metadata: %{from: MyModule.RPC}}, 500
      assert_receive %Message{body: "ADIOS", metadata: %{from: MyModule.RPC}}, 500
      assert_receive {:DOWN, ^ref_con1, :process, _, :normal}
      refute_receive {:DOWN, ^ref_con2, :process, _, :normal}
      refute_receive {:DOWN, ^ref_prod, :process, _, :normal}
    end
  end

end
