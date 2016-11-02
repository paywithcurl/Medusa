defmodule Medusa.Adapter.PG2Test do
  use ExUnit.Case
  require Medusa
  import Medusa.TestHelper
  alias Medusa.Broker.Message

  setup do
    Process.register(self, :self)
    :ok
  end

  describe "PG2 without clustering" do

    setup do
      put_adapter_config(Medusa.Adapter.PG2)
      :ok
    end

    test "Send events" do
      Medusa.consume("foo.bar", &MyModule.echo/1)
      Medusa.consume("foo.*", &MyModule.echo/1)
      Medusa.consume("foo.baz", &MyModule.echo/1)
      Medusa.publish("foo.bar", "foobar", %{"optional_field" => "nice_to_have"})
      assert_receive %Message{body: "foobar", metadata: %{"optional_field" => "nice_to_have"}}, 500
      assert_receive %Message{body: "foobar", metadata: %{"optional_field" => "nice_to_have"}}, 500
      refute_receive %Message{body: "foobar", metadata: %{"optional_field" => "nice_to_have"}}, 500
    end

    test "Send non-match events" do
      Medusa.consume("ping.pong", &MyModule.echo/1)
      Medusa.publish("ping", "ping")
      refute_receive %Message{body: "ping"}
    end

    test "Send event to consumer with bind_once: true.
          consumer and producer should die" do
      assert {:ok, %{consumer: consumer, producer: producer}} =
        Medusa.consume("local.bind1", &MyModule.echo/1, bind_once: true)
      assert Process.alive?(consumer)
      assert Process.alive?(producer)
      ref_consumer = Process.monitor(consumer)
      ref_producer = Process.monitor(producer)
      Medusa.publish("local.bind1", "die both")
      assert_receive %Message{body: "die both"}, 500
      assert_receive {:DOWN, ^ref_consumer, :process, _, :normal}, 500
      assert_receive {:DOWN, ^ref_producer, :process, _, :normal}, 500
    end

    test "Send event to consumer with bind_once: true in already exists route
          So producer is shared with others then it should not die" do
      assert {:ok, %{consumer: con1, producer: producer}} =
        Medusa.consume("local.bind2", &MyModule.echo/1)
      assert {:ok, %{consumer: con2, producer: ^producer}} =
        Medusa.consume("local.bind2", &MyModule.echo/1, bind_once: true)
      workers = Supervisor.which_children(Medusa.ConsumerSupervisor)
      assert {:undefined, con1, :worker, [Medusa.Consumer.PG2]} in workers
      assert {:undefined, con2, :worker, [Medusa.Consumer.PG2]} in workers
      assert Process.alive?(con1)
      assert Process.alive?(con2)
      assert Process.alive?(producer)
      ref_con1 = Process.monitor(con1)
      ref_con2 = Process.monitor(con2)
      ref_prod = Process.monitor(producer)
      Medusa.publish("local.bind2", "only con2 die")
      assert_receive %Message{body: "only con2 die"}, 500
      assert_receive %Message{body: "only con2 die"}, 500
      refute_receive {:DOWN, ^ref_con1, :process, _, :normal}, 500
      assert_receive {:DOWN, ^ref_con2, :process, _, :normal}, 500
      refute_receive {:DOWN, ^ref_prod, :process, _, :normal}, 500
    end

    test "Send event to consumer with bind_once: true and then
          start consume with long-running consumer, producer should survive" do
      assert {:ok, %{consumer: con1, producer: producer}} =
         Medusa.consume("local.bind3", &MyModule.echo/1, bind_once: true)
      assert {:ok, %{consumer: con2, producer: ^producer}} =
         Medusa.consume("local.bind3", &MyModule.echo/1)
      workers = Supervisor.which_children(Medusa.ConsumerSupervisor)
      assert {:undefined, con1, :worker, [Medusa.Consumer.PG2]} in workers
      assert {:undefined, con2, :worker, [Medusa.Consumer.PG2]} in workers
      assert Process.alive?(con1)
      assert Process.alive?(con2)
      assert Process.alive?(producer)
      ref_con1 = Process.monitor(con1)
      ref_con2 = Process.monitor(con2)
      ref_prod = Process.monitor(producer)
      Medusa.publish("local.bind3", "only con1 die")
      assert_receive %Message{body: "only con1 die"}, 500
      assert_receive %Message{body: "only con1 die"}, 500
      assert_receive {:DOWN, ^ref_con1, :process, _, :normal}, 500
      refute_receive {:DOWN, ^ref_con2, :process, _, :normal}, 500
      refute_receive {:DOWN, ^ref_prod, :process, _, :normal}, 500
    end
  end

  describe "PG2 Adapter with clustered" do
    setup do
      put_adapter_config(Medusa.Adapter.PG2)
      Medusa.Cluster.spawn
      {:ok, node1: :'node1@127.0.0.1', node2: :'node2@127.0.0.1'}
    end

    @tag :pg2
    test "Sent events", %{node1: node1} do
      Medusa.consume("pg.bar", &MyModule.echo/1)
      Medusa.consume("pg.*", &MyModule.echo/1)
      Medusa.consume("pg.baz", &MyModule.echo/1)
      :rpc.call(node1, Medusa, :publish, ["pg.bar", "pgbar", %{"optional_field" => "nice_to_have"}])
      assert_receive %Message{body: "pgbar", metadata: %{"optional_field" => "nice_to_have"}}, 500
      assert_receive %Message{body: "pgbar", metadata: %{"optional_field" => "nice_to_have"}}, 500
      refute_receive %Message{body: "pgbar", metadata: %{"optional_field" => "nice_to_have"}}, 500
    end

    @tag :pg2
    test "Send non-match events", %{node1: node1} do
      Medusa.consume("pg.pong", &MyModule.echo/1)
      :rpc.call(node1, Medusa, :publish, ["pg.ping", "ping"])
      refute_receive %Message{body: "ping"}, 500
    end

    @tag :pg2
    test "Send event to consumer with bind_once: true.
          consumer and producer should die", %{node1: node1} do
      assert {:ok, %{consumer: consumer, producer: producer}} =
        Medusa.consume("pg.bind1", &MyModule.echo/1, bind_once: true)
      assert Process.alive?(consumer)
      assert Process.alive?(producer)
      ref_consumer = Process.monitor(consumer)
      ref_producer = Process.monitor(producer)
      :rpc.call(node1, Medusa, :publish, ["pg.bind1", "die both"])
      assert_receive %Message{body: "die both"}
      assert_receive {:DOWN, ^ref_consumer, :process, _, :normal}, 500
      assert_receive {:DOWN, ^ref_producer, :process, _, :normal}, 500
    end

    @tag :pg2
    test "Send event to consumer with bind_once: true in already exists route
          So producer is shared with others then it should not die", %{node1: node1} do
      assert {:ok, %{consumer: con1, producer: producer}} =
        Medusa.consume("pg.bind2", &MyModule.echo/1)
      assert {:ok, %{consumer: con2, producer: ^producer}} =
        Medusa.consume("pg.bind2", &MyModule.echo/1, bind_once: true)
      workers = Supervisor.which_children(Medusa.ConsumerSupervisor)
      assert {:undefined, con1, :worker, [Medusa.Consumer.PG2]} in workers
      assert {:undefined, con2, :worker, [Medusa.Consumer.PG2]} in workers
      assert Process.alive?(con1)
      assert Process.alive?(con2)
      assert Process.alive?(producer)
      ref_con1 = Process.monitor(con1)
      ref_con2 = Process.monitor(con2)
      ref_prod = Process.monitor(producer)
      :rpc.call(node1, Medusa, :publish, ["pg.bind2", "only con2 die"])
      assert_receive %Message{body: "only con2 die"}, 500
      assert_receive %Message{body: "only con2 die"}, 500
      refute_receive {:DOWN, ^ref_con1, :process, _, :normal}, 500
      assert_receive {:DOWN, ^ref_con2, :process, _, :normal}, 500
      refute_receive {:DOWN, ^ref_prod, :process, _, :normal}, 500
    end

    @tag :pg2
    test "Send event to consumer with bind_once: true and then
          start consume with long-running consumer, producer should survive", %{node1: node1} do
      assert {:ok, %{consumer: con1, producer: producer}} =
         Medusa.consume("pg.bind3", &MyModule.echo/1, bind_once: true)
      assert {:ok, %{consumer: con2, producer: ^producer}} =
         Medusa.consume("pg.bind3", &MyModule.echo/1)
      workers = Supervisor.which_children(Medusa.ConsumerSupervisor)
      assert {:undefined, con1, :worker, [Medusa.Consumer.PG2]} in workers
      assert {:undefined, con2, :worker, [Medusa.Consumer.PG2]} in workers
      assert Process.alive?(con1)
      assert Process.alive?(con2)
      assert Process.alive?(producer)
      ref_con1 = Process.monitor(con1)
      ref_con2 = Process.monitor(con2)
      ref_prod = Process.monitor(producer)
      :rpc.call(node1, Medusa, :publish, ["pg.bind3", "only con1 die"])
      assert_receive %Message{body: "only con1 die"}, 500
      assert_receive %Message{body: "only con1 die"}, 500
      assert_receive {:DOWN, ^ref_con1, :process, _, :normal}, 500
      refute_receive {:DOWN, ^ref_con2, :process, _, :normal}, 500
      refute_receive {:DOWN, ^ref_prod, :process, _, :normal}, 500
    end
  end
end
