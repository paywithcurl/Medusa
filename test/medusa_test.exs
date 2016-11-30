defmodule MedusaTest do
  use ExUnit.Case
  doctest Medusa

  test "not config should fallback to default" do
    Application.delete_env(:medusa, Medusa, persistent: true)
    Application.stop(:medusa)
    Application.ensure_all_started(:medusa)
    assert Application.get_env(:medusa, Medusa) ==
      [adapter: Medusa.Adapter.PG2]
  end

  test "config invalid adapter should fallback to PG2" do
    Application.put_env(:medusa, Medusa, [adapter: Wrong], persistent: true)
    Application.stop(:medusa)
    Application.ensure_all_started(:medusa)
    assert Application.get_env(:medusa, Medusa) ==
      [adapter: Medusa.Adapter.PG2]
  end

  test "Add consumers" do
    before = Medusa.ConsumerSupervisor |> Supervisor.which_children |> length
    Medusa.consume("foo.bob", &IO.puts/1)
    afters = Medusa.ConsumerSupervisor |> Supervisor.which_children |> length
    assert afters - before == 1
    assert Process.whereis(:"foo.bob")
  end

  test "Add invalid consumer" do
    assert Medusa.consume("foo.bob", fn -> IO.puts("blah") end)
  end

  test "Don't publish when validator rejects message" do
    validator = fn _, _, _ -> {:error, "failed"} end
    MedusaConfig.set_message_validator(:medusa_config, validator)
    result = Medusa.publish "validator.rejected", %{}, %{}
    MedusaConfig.set_message_validator(:medusa_config, nil)
    assert result == {:error, "message is invalid"}
  end

  test "Publish when validator accepts message" do
    validator = fn _, _, _ -> :ok end
    MedusaConfig.set_message_validator(:medusa_config, validator)
    result = Medusa.publish "validator.accepted", %{}, %{}
    MedusaConfig.set_message_validator(:medusa_config, nil)
    assert result == :ok
  end

  test "Publish adds an id in metadata if not present" do
    MedusaConfig.set_message_validator(:medusa_config, &ensures_id_present/3)
    result = Medusa.publish "validator.accepted", %{}, %{}
    MedusaConfig.set_message_validator(:medusa_config, nil)
    assert result == :ok
  end

  test "Publish leaves id in metadata if present" do
    MedusaConfig.set_message_validator(:medusa_config, &ensures_id_1234/3)
    result = Medusa.publish "validator.accepted", %{}, %{:id => 1234, :test => "blah"}
    MedusaConfig.set_message_validator(:medusa_config, nil)
    assert result == :ok
  end

  defp ensures_id_1234(_, _, %{id: 1234}) do
    :ok
  end

  defp ensures_id_present(_, _, %{id: _}) do
    :ok
  end

end
