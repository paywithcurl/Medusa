defmodule MyModule do
  def echo(message) do
    :self |> Process.whereis |> send(message)
  end
end

#   # describe "RabbitMQ Adapter" do
#   #   setup do
#   #     put_adapter_config(Medusa.Adapter.RabbitMQ)
#   #     Process.register(self, :self)
#   #     :ok
#   #   end
#   #
#   #   @tag :rabbitmq
#   #   test "sent events" do
#   #     Medusa.consume("rabbitmq", &MyModule.rpc/1)
#   #     spawn fn -> Medusa.publish("rabbitmq", "RABBIT!") end
#   #     assert_receive %Message{body: "RABBIT!", metadata: %{from: MyModule.RPC}}, 500
#   #   end
#   #
#   #   @tag :rabbitmq
#   #   test "Send event to consumer with bind_once: true.
#   #         consumer and producer should die" do
#   #     assert {:ok, consumer} = Medusa.consume("rabbitmq.1", &MyModule.rpc/1, bind_once: true)
#   #     assert Process.alive?(consumer)
#   #     producer = Process.whereis(:"rabbitmq.1")
#   #     assert Process.alive?(producer)
#   #     ref_consumer = Process.monitor(consumer)
#   #     ref_producer = Process.monitor(producer)
#   #     spawn fn -> Medusa.publish("rabbitmq.1", "HOLA") end
#   #     assert_receive %Message{body: "HOLA", metadata: %{from: MyModule.RPC}}, 500
#   #     assert_receive {:DOWN, ^ref_consumer, :process, _, :normal}
#   #     assert_receive {:DOWN, ^ref_producer, :process, _, :normal}
#   #   end
#   #
#   #   @tag :rabbitmq
#   #   test "Send event to consumer with bind_once: true in already exists route
#   #         So producer is shared with others then it should not die" do
#   #     assert {:ok, con1} = Medusa.consume("rabbitmq.2", &MyModule.rpc/1)
#   #     assert {:ok, con2} = Medusa.consume("rabbitmq.2", &MyModule.rpc/1, bind_once: true)
#   #     assert Process.alive?(con1)
#   #     assert Process.alive?(con2)
#   #     producer = Process.whereis(:"rabbitmq.2")
#   #     assert Process.alive?(producer)
#   #     ref_con1 = Process.monitor(con1)
#   #     ref_con2 = Process.monitor(con2)
#   #     ref_prod = Process.monitor(producer)
#   #     spawn fn -> Medusa.publish("rabbitmq.2", "GRACIAS") end
#   #     assert_receive %Message{body: "GRACIAS", metadata: %{from: MyModule.RPC}}, 500
#   #     assert_receive %Message{body: "GRACIAS", metadata: %{from: MyModule.RPC}}, 500
#   #     refute_receive {:DOWN, ^ref_con1, :process, _, :normal}
#   #     assert_receive {:DOWN, ^ref_con2, :process, _, :normal}
#   #     refute_receive {:DOWN, ^ref_prod, :process, _, :normal}
#   #   end
#   #
#   #   @tag :rabbitmq
#   #   test "Send event to consumer with bind_once: true and then
#   #         start consume with long-running consumer, producer should survive" do
#   #     assert {:ok, con1} = Medusa.consume("rabbitmq.3", &MyModule.rpc/1, bind_once: true)
#   #     assert {:ok, con2} = Medusa.consume("rabbitmq.3", &MyModule.rpc/1)
#   #     assert Process.alive?(con1)
#   #     assert Process.alive?(con2)
#   #     producer = Process.whereis(:"rabbitmq.3")
#   #     assert Process.alive?(producer)
#   #     ref_con1 = Process.monitor(con1)
#   #     ref_con2 = Process.monitor(con2)
#   #     ref_prod = Process.monitor(producer)
#   #     spawn fn -> Medusa.publish("rabbitmq.3", "ADIOS") end
#   #     assert_receive %Message{body: "ADIOS", metadata: %{from: MyModule.RPC}}, 500
#   #     assert_receive %Message{body: "ADIOS", metadata: %{from: MyModule.RPC}}, 500
#   #     assert_receive {:DOWN, ^ref_con1, :process, _, :normal}
#   #     refute_receive {:DOWN, ^ref_con2, :process, _, :normal}
#   #     refute_receive {:DOWN, ^ref_prod, :process, _, :normal}
#   #   end
#   # end
#
# end
