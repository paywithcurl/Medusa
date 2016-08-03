defmodule ExternalIntegrationTest do
  use ExUnit.Case
  require Medusa

  test "Add consumers" do
    assert {:ok, _pid} = Medusa.consume ~r/^foo\.bar$/, IO, :inspect
    assert {:ok, _pid} = Medusa.consume ~r/^foo\.bar$/, IO, :inspect
    assert {:ok, _pid} = Medusa.consume ~r/^foo\.biz$/, IO, :inspect
    assert {:ok, _pid} = Medusa.consume ~r/^foo\.*/, IO, :inspect
    assert {:ok, _pid} = Medusa.consume ~r/^foo\.beeb$/, IO, :inspect
    assert {:ok, _pid} = Medusa.consume ~r/^foo\.piip$/, IO, :inspect
    assert {:ok, _pid} = Medusa.consume ~r/^foo\.buh$/, IO, :inspect
  end

  test "Send events" do
    Medusa.publish "foo.bar", 90
    Medusa.publish "foo.buh", 100
    Medusa.publish "foo.beeb", 60
    Medusa.publish "foo.biz", 1000
    Medusa.publish "foo.beeb", 76
    Medusa.publish "foo.biz", 1542
  end
  
end