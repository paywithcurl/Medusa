defmodule ExternalIntegrationTest do
  use ExUnit.Case
  require Medusa

  test "Add consumers" do
    assert {:ok, _pid} = Medusa.consume ~r/^foo\.bar$/, IO, :inspect
    assert {:ok, _pid} = Medusa.consume ~r/^foo\.biz$/, IO, :inspect
    assert {:ok, _pid} = Medusa.consume ~r/^foo\.*/, IO, :inspect
    assert {:ok, _pid} = Medusa.consume ~r/^foo\.beeb$/, IO, :inspect
    assert {:ok, _pid} = Medusa.consume ~r/^foo\.piip$/, IO, :inspect
    assert {:ok, _pid} = Medusa.consume ~r/^foo\.buh$/, IO, :inspect
  end

  test "Send events" do
    Medusa.Broker.publish "foo.bar", 90
    Medusa.Broker.publish "foo.buh", 100
    Medusa.Broker.publish "foo.beeb", 60
    Medusa.Broker.publish "foo.biz", 1000
    Medusa.Broker.publish "foo.beeb", 76
    Medusa.Broker.publish "foo.biz", 1542
  end
  
end